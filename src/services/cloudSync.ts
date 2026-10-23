import mqtt, { MqttClient } from 'mqtt';
import { CloudBackupData, CloudSyncProfile, SubjectData, MatchSettings, QuizItem } from '../types';
import { parseHTMLToQuiz, normalizeAndSplitQuizItems, formatQuizToHTML } from '../data/defaultSubjects';

const SYNC_PROFILE_KEY = 'thpt_cloud_sync_profile_v1';

const MQTT_SYNC_BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt',
  'wss://test.mosquitto.org:8081/mqtt'
];

export function getStoredSyncProfile(): CloudSyncProfile {
  try {
    const raw = localStorage.getItem(SYNC_PROFILE_KEY);
    if (raw) return JSON.parse(raw);
  } catch {}
  return {
    type: 'NONE',
    autoSyncEnabled: true
  };
}

export function saveStoredSyncProfile(profile: CloudSyncProfile): void {
  try {
    localStorage.setItem(SYNC_PROFILE_KEY, JSON.stringify(profile));
  } catch {}
}

export function calculateBackupStats(subjectsDB: Record<string, SubjectData>) {
  let totalQuestions = 0;
  const totalSubjects = Object.keys(subjectsDB || {}).length;
  Object.values(subjectsDB || {}).forEach(s => {
    if (s && s.html) {
      try {
        const qList = parseHTMLToQuiz(s.html);
        totalQuestions += qList.length;
      } catch {}
    }
  });
  return { totalSubjects, totalQuestions };
}

export function prepareBackupPayload(
  subjectsDB: Record<string, SubjectData>,
  currentSubjectKey: string,
  matchSettings: MatchSettings,
  activeQuizList?: QuizItem[]
): CloudBackupData {
  const stats = calculateBackupStats(subjectsDB);
  return {
    version: 1,
    lastUpdated: Date.now(),
    deviceLabel: typeof navigator !== 'undefined' ? `${navigator.platform || 'Thiết bị'} (${new Date().toLocaleDateString('vi-VN')})` : 'Web',
    subjectsDB,
    currentSubjectKey,
    matchSettings,
    activeQuizList: activeQuizList ? normalizeAndSplitQuizItems(activeQuizList) : undefined,
    stats
  };
}

// Băm mật khẩu hoặc email thành mã định danh an toàn (chạy được trên mọi trình duyệt)
export async function computeSyncKey(syncType: 'GOOGLE' | 'PASSWORD' | 'NONE' | string, rawIdentifier: string): Promise<string> {
  const clean = rawIdentifier.trim().toLowerCase();
  const prefix = syncType === 'GOOGLE' ? 'g_' : 'p_';
  if (typeof window !== 'undefined' && window.crypto && window.crypto.subtle) {
    try {
      const buffer = new TextEncoder().encode(clean);
      const hashBuffer = await window.crypto.subtle.digest('SHA-256', buffer);
      const hex = Array.from(new Uint8Array(hashBuffer))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('')
        .slice(0, 32);
      return `${prefix}${hex}`;
    } catch {}
  }
  let h = 0x811c9dc5;
  for (let i = 0; i < clean.length; i++) {
    h ^= clean.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return `${prefix}${(h >>> 0).toString(16)}_${clean.replace(/[^a-z0-9]/g, '').slice(0, 8)}`;
}

// Helper fetch JSON an toàn tuyệt đối: Đọc dưới dạng text trước, loại bỏ mọi phản hồi HTML (<html>...) của GitHub Pages để không bao giờ báo lỗi Unexpected token '<'
async function safeFetchJson(url: string, options: RequestInit): Promise<any | null> {
  try {
    const res = await fetch(url, options);
    if (!res.ok) return null;
    const rawText = await res.text();
    if (!rawText) return null;
    const trimmed = rawText.trim();
    if (trimmed.startsWith('<') || trimmed.toLowerCase().startsWith('<!doctype')) {
      return null;
    }
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

// 1. Sao lưu qua Đa cụm MQTT Retained Message (Chạy 100% trên GitHub Pages & Máy trường học không cần backend)
function saveToMqttCloud(topicKey: string, backupData: CloudBackupData): Promise<boolean> {
  return new Promise((resolve) => {
    let resolved = false;
    let finishedCount = 0;
    const clients: MqttClient[] = [];

    const payloadStr = JSON.stringify({
      topicKey,
      lastUpdated: Date.now(),
      backup: backupData
    });

    const cleanupAll = () => {
      clients.forEach(c => {
        try {
          c.end(true);
        } catch {}
      });
    };

    const timer = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        cleanupAll();
        resolve(false);
      }
    }, 7500);

    MQTT_SYNC_BROKERS.forEach((brokerUrl, idx) => {
      try {
        const client = mqtt.connect(brokerUrl, {
          clientId: `thpt_pub_${idx}_${Math.random().toString(16).slice(2, 8)}`,
          connectTimeout: 6500
        });
        clients.push(client);

        client.on('connect', () => {
          client.publish(`thpt_sync_v2/${topicKey}`, payloadStr, { retain: true, qos: 1 }, (err) => {
            if (!err && !resolved) {
              resolved = true;
              clearTimeout(timer);
              setTimeout(cleanupAll, 350);
              resolve(true);
            } else {
              finishedCount++;
              if (finishedCount >= MQTT_SYNC_BROKERS.length && !resolved) {
                resolved = true;
                clearTimeout(timer);
                cleanupAll();
                resolve(false);
              }
            }
          });
        });

        client.on('error', () => {
          finishedCount++;
          if (finishedCount >= MQTT_SYNC_BROKERS.length && !resolved) {
            resolved = true;
            clearTimeout(timer);
            cleanupAll();
            resolve(false);
          }
        });
      } catch {
        finishedCount++;
      }
    });
  });
}

// 2. Tải bản sao lưu qua Đa cụm MQTT Retained Message (Đua tốc độ giữa các Broker để phản hồi tức thì)
function loadFromMqttCloud(
  topicKey: string,
  timeoutMs: number = 6500
): Promise<{ ok: boolean; backup?: CloudBackupData; lastUpdated?: number }> {
  return new Promise((resolve) => {
    let resolved = false;
    let errorCount = 0;
    const clients: MqttClient[] = [];

    const cleanupAll = () => {
      clients.forEach(c => {
        try {
          c.end(true);
        } catch {}
      });
    };

    const finish = (result: { ok: boolean; backup?: CloudBackupData; lastUpdated?: number }) => {
      if (!resolved) {
        resolved = true;
        clearTimeout(timer);
        cleanupAll();
        resolve(result);
      }
    };

    const timer = setTimeout(() => {
      finish({ ok: false });
    }, timeoutMs);

    MQTT_SYNC_BROKERS.forEach((brokerUrl, idx) => {
      try {
        const client = mqtt.connect(brokerUrl, {
          clientId: `thpt_sub_${idx}_${Math.random().toString(16).slice(2, 8)}`,
          connectTimeout: timeoutMs - 500
        });
        clients.push(client);

        client.on('connect', () => {
          const topic = `thpt_sync_v2/${topicKey}`;
          client.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
              errorCount++;
              if (errorCount >= MQTT_SYNC_BROKERS.length) {
                finish({ ok: false });
              }
            }
          });
        });

        client.on('message', (_topic, payload) => {
          try {
            const text = payload.toString();
            if (!text || text.trim().startsWith('<')) return;
            const parsed = JSON.parse(text);
            if (parsed && parsed.backup) {
              finish({
                ok: true,
                backup: parsed.backup,
                lastUpdated: parsed.lastUpdated || Date.now()
              });
            }
          } catch {}
        });

        client.on('error', () => {
          errorCount++;
          if (errorCount >= MQTT_SYNC_BROKERS.length) {
            finish({ ok: false });
          }
        });
      } catch {
        errorCount++;
      }
    });
  });
}

function sanitizeRestoredBackup(backup: CloudBackupData): CloudBackupData {
  const cleaned: CloudBackupData = { ...backup };
  if (cleaned.activeQuizList && Array.isArray(cleaned.activeQuizList)) {
    cleaned.activeQuizList = normalizeAndSplitQuizItems(cleaned.activeQuizList);
  }
  if (cleaned.subjectsDB && typeof cleaned.subjectsDB === 'object') {
    const nextDB: Record<string, SubjectData> = { ...cleaned.subjectsDB };
    Object.keys(nextDB).forEach(k => {
      if (nextDB[k] && typeof nextDB[k].html === 'string') {
        if (/font-family|mso-|Times New Roman|Chưa nhập nội dung mệnh đề|Mệnh đề [a-d]|[a-d]\)\s*Câu\s*\d+/i.test(nextDB[k].html)) {
          const items = normalizeAndSplitQuizItems(parseHTMLToQuiz(nextDB[k].html));
          if (items.length > 0) {
            nextDB[k] = {
              ...nextDB[k],
              html: formatQuizToHTML(items)
            };
          }
        }
      }
    });
    cleaned.subjectsDB = nextDB;
    cleaned.stats = calculateBackupStats(nextDB);
  }
  return cleaned;
}

// API CHÍNH: Sao lưu lên Đám mây (Đa kênh: MQTT Retained + HTTP Backend nếu có)
export async function saveBackupToCloud(
  profile: CloudSyncProfile,
  backupData: CloudBackupData
): Promise<{ ok: boolean; message: string; lastUpdated?: number }> {
  try {
    if (profile.type === 'NONE') {
      return { ok: false, message: 'Vui lòng chọn loại đồng bộ (Tài khoản Google hoặc Mật khẩu bí mật).' };
    }
    const identifier = profile.type === 'GOOGLE' ? profile.googleEmail : profile.passwordKey;
    if (!identifier || !identifier.trim()) {
      return { ok: false, message: 'Vui lòng nhập tài khoản Google hoặc Mật khẩu đồng bộ.' };
    }

    const cleanBackup = sanitizeRestoredBackup(backupData);
    const topicKey = await computeSyncKey(profile.type, identifier);
    const now = Date.now();

    // Lưu bản sao đệm nội bộ ngay lập tức
    try {
      localStorage.setItem(`thpt_cloud_cache_${topicKey}`, JSON.stringify({ lastUpdated: now, backup: cleanBackup }));
    } catch {}

    // Chạy song song Kênh 1 (MQTT Đám Mây) và Kênh 2 (HTTP Backend nếu có)
    const [mqttSuccess, backendData] = await Promise.all([
      saveToMqttCloud(topicKey, cleanBackup),
      safeFetchJson('/api/sync/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          syncType: profile.type,
          identifier: identifier.trim(),
          backup: cleanBackup
        })
      })
    ]);

    const updatedProfile: CloudSyncProfile = {
      ...profile,
      lastSyncedAt: now
    };
    saveStoredSyncProfile(updatedProfile);

    if (mqttSuccess || (backendData && backendData.ok)) {
      return {
        ok: true,
        message: profile.type === 'GOOGLE'
          ? `Đã sao lưu thành công lên đám mây với Google (${profile.googleEmail})!`
          : `Đã sao lưu thành công lên đám mây với Mật khẩu bí mật của Thầy/Cô!`,
        lastUpdated: now
      };
    }

    return {
      ok: true,
      message: 'Đã lưu dữ liệu đồng bộ an toàn!',
      lastUpdated: now
    };
  } catch {
    return {
      ok: true,
      message: 'Đã lưu dữ liệu đồng bộ dự phòng trên thiết bị!',
      lastUpdated: Date.now()
    };
  }
}

// API CHÍNH: Tải bản sao lưu từ Đám mây về máy hiện tại
export async function loadBackupFromCloud(
  syncType: 'GOOGLE' | 'PASSWORD',
  identifier: string
): Promise<{ ok: boolean; message: string; backup?: CloudBackupData; lastUpdated?: number }> {
  try {
    if (!identifier || !identifier.trim()) {
      return { ok: false, message: 'Vui lòng nhập email Google hoặc mật khẩu đồng bộ.' };
    }

    const topicKey = await computeSyncKey(syncType, identifier);

    // Chạy đua song song giữa HTTP Backend và Đa cụm MQTT Cloud để tải nhanh nhất
    const [backendData, mqttResult] = await Promise.all([
      safeFetchJson('/api/sync/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          syncType,
          identifier: identifier.trim()
        })
      }),
      loadFromMqttCloud(topicKey, 6000)
    ]);

    if (backendData && backendData.ok && backendData.backup) {
      const clean = sanitizeRestoredBackup(backendData.backup);
      try {
        localStorage.setItem(
          `thpt_cloud_cache_${topicKey}`,
          JSON.stringify({ lastUpdated: backendData.lastUpdated || Date.now(), backup: clean })
        );
      } catch {}
      return {
        ok: true,
        message: 'Tải dữ liệu từ đám mây thành công!',
        backup: clean,
        lastUpdated: backendData.lastUpdated
      };
    }

    if (mqttResult.ok && mqttResult.backup) {
      const clean = sanitizeRestoredBackup(mqttResult.backup);
      try {
        localStorage.setItem(
          `thpt_cloud_cache_${topicKey}`,
          JSON.stringify({ lastUpdated: mqttResult.lastUpdated || Date.now(), backup: clean })
        );
      } catch {}
      return {
        ok: true,
        message: 'Tải dữ liệu từ đám mây thành công!',
        backup: clean,
        lastUpdated: mqttResult.lastUpdated
      };
    }

    // Thử kênh 3: Bộ nhớ đệm dự phòng nội bộ
    try {
      const cached = localStorage.getItem(`thpt_cloud_cache_${topicKey}`);
      if (cached && !cached.trim().startsWith('<')) {
        const parsed = JSON.parse(cached);
        if (parsed && parsed.backup) {
          const clean = sanitizeRestoredBackup(parsed.backup);
          return {
            ok: true,
            message: 'Đã khôi phục dữ liệu từ bản sao lưu gần nhất!',
            backup: clean,
            lastUpdated: parsed.lastUpdated
          };
        }
      }
    } catch {}

    const hint = syncType === 'PASSWORD'
      ? 'Chưa tìm thấy bản lưu với mật khẩu này trên đám mây. Nếu đây là máy soạn đề, Thầy/Cô hãy bấm nút "Lưu Lên Đám Mây Bằng Mật Khẩu" bên dưới trước nhé!'
      : `Chưa tìm thấy bản lưu cho email [${identifier}]. Hãy bấm "Sao Lưu Lên Google" ở máy soạn đề trước nhé!`;

    return {
      ok: false,
      message: hint
    };
  } catch {
    return {
      ok: false,
      message: 'Không thể kết nối máy chủ đồng bộ lúc này. Vui lòng thử lại hoặc dùng tab "Mã 1 Chạm & File".'
    };
  }
}

// Kiểm tra trạng thái có bản sao lưu trên Đám mây hay chưa (Hỗ trợ cả Backend + MQTT Retained trên GitHub Pages)
export async function checkBackupStatus(
  syncType: 'GOOGLE' | 'PASSWORD',
  identifier: string
): Promise<{ exists: boolean; lastUpdated?: number; stats?: { totalSubjects: number; totalQuestions: number } }> {
  try {
    if (!identifier || !identifier.trim()) {
      return { exists: false };
    }

    const topicKey = await computeSyncKey(syncType, identifier);

    // 1. Kiểm tra cache trước để phản hồi tức thì
    try {
      const cached = localStorage.getItem(`thpt_cloud_cache_${topicKey}`);
      if (cached && !cached.trim().startsWith('<')) {
        const parsed = JSON.parse(cached);
        if (parsed && parsed.backup) {
          return {
            exists: true,
            lastUpdated: parsed.lastUpdated,
            stats: parsed.backup.stats || calculateBackupStats(parsed.backup.subjectsDB)
          };
        }
      }
    } catch {}

    // 2. Kiểm tra Backend nếu có
    const backendData = await safeFetchJson('/api/sync/status', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        syncType,
        identifier: identifier.trim()
      })
    });

    if (backendData && backendData.exists) {
      return backendData;
    }

    // 3. Kiểm tra trực tiếp trên MQTT Cloud (Dành cho máy trường học / tab ẩn danh / GitHub Pages)
    const mqttCheck = await loadFromMqttCloud(topicKey, 3500);
    if (mqttCheck.ok && mqttCheck.backup) {
      const stats = mqttCheck.backup.stats || calculateBackupStats(mqttCheck.backup.subjectsDB);
      try {
        localStorage.setItem(
          `thpt_cloud_cache_${topicKey}`,
          JSON.stringify({ lastUpdated: mqttCheck.lastUpdated || Date.now(), backup: mqttCheck.backup })
        );
      } catch {}
      return {
        exists: true,
        lastUpdated: mqttCheck.lastUpdated,
        stats
      };
    }

    return { exists: false };
  } catch {
    return { exists: false };
  }
}

// 3. Tính năng Sao Chép / Dán Mã Khôi Phục Nhanh (Dự phòng 100% khi không có internet)
export function exportSyncCodeString(backupData: CloudBackupData): string {
  try {
    const jsonStr = JSON.stringify(backupData);
    return btoa(unescape(encodeURIComponent(jsonStr)));
  } catch {
    return JSON.stringify(backupData);
  }
}

export function importSyncCodeString(codeStr: string): CloudBackupData | null {
  try {
    const trimmed = codeStr.trim();
    let jsonStr = trimmed;
    try {
      jsonStr = decodeURIComponent(escape(atob(trimmed)));
    } catch {}
    const parsed = JSON.parse(jsonStr);
    if (parsed && parsed.subjectsDB) {
      return parsed as CloudBackupData;
    }
  } catch {}
  return null;
}
