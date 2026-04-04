const SHEET_NAME = 'raw';
const WORKOUTS_RAW_SHEET_NAME = 'raw_workouts';
const WORKOUTS_SHEET_NAME = 'workouts';
const DEBUG_SHEET_NAME = 'debug';
const DASHBOARD_SHEET_NAME = 'dashboard';
const STATS_SHEET_NAME = 'stats';
const READINESS_SHEET_NAME = 'readiness_50d';
const DASHBOARD_REFRESH_FUNCTION = 'refreshDashboardAndReadiness_';
const DASHBOARD_REFRESH_TRIGGER_HOURS = [0, 6, 12, 18];
const TOKEN = '2004';
const MAX_CELL_CHARS = 45000;
const MAX_PAYLOAD_BYTES = 5 * 1024 * 1024;
const DATE_FIELDS = new Set(['date', 'date_iso', 'received_at', 'sleepStart', 'sleepEnd', 'inBedStart', 'inBedEnd', 'start_iso', 'end_iso', 'start', 'end', 'startDate', 'endDate']);
const DECIMAL2_FIELDS = new Set(['totalSleep', 'totalSleep_num', 'deep', 'deep_num', 'rem', 'rem_num', 'core', 'core_num', 'awake', 'awake_num', 'inBed', 'inBed_num', 'Min', 'Min_num', 'Max', 'Max_num', 'Avg', 'Avg_num', 'duration_min', 'distance_km', 'energy_kcal', 'avg_hr', 'max_hr', 'min_hr']);
const READINESS_HEADERS = [
  'Fecha (DD-MM-YYYY)',
  'Sleep_hours',
  'Sleep_quality',
  'Readiness_score',
  'HR_resting',
  'HRV',
  'Fatiga_percibida',
  'Energia_autoinformada',
  'Peso_kg',
  'Fuente',
  'Ultima_actualizacion (HH:MM DD-MM-YYYY)',
  'Notas'
];

const DERIVED_SHEETS = {
  peso: ['weight_body_mass'],
  cardio: [
    'walking_heart_rate_average',
    'resting_heart_rate',
    'vo2_max',
    'heart_rate',
    'heart_rate_variability_sdnn',
    'walking_speed',
    'walking_step_length',
    'walking_asymmetry_percentage',
    'walking_double_support_percentage'
  ],
  sueno: ['sleep_analysis']
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Health')
    .addItem('Actualizar Dashboard', 'updateDashboardFromMenu_')
    .addItem('Recalcular kcal workouts existentes', 'recalculateExistingWorkoutEnergyFromMenu_')
    .addItem('Configurar actualización 4x día', 'setupQuarterDailyRefreshTriggers_')
    .addToUi();
}

function refreshDashboardAndReadiness_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  rebuildDashboard_(ss);
  rebuildReadiness50dSheet_(ss);
  rebuildStatsSheet_(ss);
}

function setupQuarterDailyRefreshTriggers_() {
  ensureQuarterDailyRefreshTriggers_();
  SpreadsheetApp.getActiveSpreadsheet().toast('Actualización programada 4 veces al día', 'Health', 5);
}

function ensureQuarterDailyRefreshTriggers_() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === DASHBOARD_REFRESH_FUNCTION) {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  DASHBOARD_REFRESH_TRIGGER_HOURS.forEach(hour => {
    ScriptApp.newTrigger(DASHBOARD_REFRESH_FUNCTION)
      .timeBased()
      .everyDays(1)
      .atHour(hour)
      .nearMinute(5)
      .create();
  });
}


function updateDashboardFromMenu_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  recalculateExistingWorkoutEnergyKcal_(ss);
  rebuildDerivedSheets_(ss);
  rebuildWorkoutsSheet_(ss);
  rebuildDashboard_(ss);
  rebuildStatsSheet_(ss);
  rebuildReadiness50dSheet_(ss);
  ss.toast('Dashboard actualizado', 'Health', 5);
}

function recalculateExistingWorkoutEnergyFromMenu_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const updated = recalculateExistingWorkoutEnergyKcal_(ss);
  rebuildWorkoutsSheet_(ss);
  rebuildStatsSheet_(ss);
  ss.toast('Workouts recalculados: ' + updated, 'Health', 5);
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    logDebug_('START', {
      hasEvent: !!e,
      parameter: safeJsonStringify_(e && e.parameter ? e.parameter : {}),
      contentType: e && e.postData ? e.postData.type : '',
      contentLength: e && e.postData && e.postData.contents ? String(e.postData.contents).length : 0,
      bodyPreview: truncateForCell_(e && e.postData && e.postData.contents ? e.postData.contents : '', 2000)
    });

    if (!e || !e.postData || !e.postData.contents) {
      logDebug_('ERROR', { message: 'Sin body en la petición' });
      return jsonResponse_({ ok: false, error: 'Sin body en la petición' }, 400);
    }

    const tokenFromParam = e.parameter && e.parameter.token ? String(e.parameter.token).trim() : '';
    if (tokenFromParam !== TOKEN) {
      logDebug_('ERROR', {
        message: 'No autorizado',
        tokenRecibido: tokenFromParam
      });
      return jsonResponse_({ ok: false, error: 'No autorizado' }, 401);
    }

    const contentType = String(e.postData.type || '').toLowerCase();
    const body = e.postData.contents;
    if (String(body).length > MAX_PAYLOAD_BYTES) {
      logDebug_('ERROR', { message: 'Payload demasiado grande', bytes: String(body).length });
      return jsonResponse_({ ok: false, error: 'Payload demasiado grande' }, 413);
    }

    let payload;
    if (contentType.includes('application/json') || looksLikeJson_(body)) {
      payload = JSON.parse(body);
    } else {
      logDebug_('ERROR', {
        message: 'Formato no soportado',
        contentType: contentType
      });
      return jsonResponse_({ ok: false, error: 'Formato no soportado. Usa JSON.' }, 415);
    }

    logDebug_('PAYLOAD_SHAPE', {
      topKeys: Object.keys(payload || {}),
      dataType: payload && payload.data ? (Array.isArray(payload.data) ? 'array' : typeof payload.data) : 'sin data',
      dataKeys: payload && payload.data && typeof payload.data === 'object' && !Array.isArray(payload.data)
        ? Object.keys(payload.data)
        : []
    });

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const dataset = e.parameter && e.parameter.dataset ? String(e.parameter.dataset).trim().toLowerCase() : 'health';
    const isWorkouts = dataset === 'workouts';
    const targetRawSheetName = isWorkouts ? WORKOUTS_RAW_SHEET_NAME : SHEET_NAME;
    const rawSheet = getOrCreateSheet_(ss, targetRawSheetName);

    const now = new Date();
    let rows = isWorkouts ? normalizeWorkoutPayloadToRows_(payload, now) : normalizePayloadToRows_(payload, now);
    rows = rows.map(isWorkouts ? enrichWorkoutRow_ : enrichRow_);
    if (isWorkouts) {
      const fallbackWeightKg = getLatestWeightKgFromRaw_(ss);
      rows = rows.map(row => enrichWorkoutEnergyWithFallbackWeight_(row, fallbackWeightKg));
    }

    logDebug_('ROWS', {
      rowsLength: rows.length,
      firstRow: rows.length ? truncateForCell_(safeJsonStringify_(rows[0]), 5000) : ''
    });

    if (!rows.length) {
      return jsonResponse_({
        ok: true,
        inserted: 0,
        skipped_duplicates: 0,
        message: 'Sin filas para insertar'
      }, 200);
    }

    ensureHeader_(rawSheet, collectAllKeys_(rows));

    const existingKeys = getExistingDedupeKeys_(rawSheet);
    const newRows = [];
    let skippedDuplicates = 0;

    rows.forEach(row => {
      const key = row.dedupe_key || (isWorkouts ? buildWorkoutDedupeKey_(row) : buildDedupeKey_(row));
      row.dedupe_key = key;

      if (existingKeys[key]) {
        skippedDuplicates++;
      } else {
        existingKeys[key] = true;
        newRows.push(row);
      }
    });

    let inserted = 0;

    if (newRows.length) {
      ensureHeader_(rawSheet, collectAllKeys_(newRows));
      const header = rawSheet.getRange(1, 1, 1, rawSheet.getLastColumn()).getValues()[0];
      const values = newRows.map(row => header.map(col => sanitizeCellValue_(row[col])));
      rawSheet.getRange(rawSheet.getLastRow() + 1, 1, values.length, header.length).setValues(values);
      inserted = values.length;
    }

    if (inserted > 0) {
      if (isWorkouts) {
        rebuildWorkoutsSheet_(ss);
      } else {
        rebuildDerivedSheets_(ss);
        rebuildDashboard_(ss);
      }
      rebuildStatsSheet_(ss);
      rebuildReadiness50dSheet_(ss);
    }

    logDebug_('SUCCESS', {
      inserted: inserted,
      skippedDuplicates: skippedDuplicates,
      lastRow: rawSheet.getLastRow()
    });

    return jsonResponse_({
      ok: true,
      inserted: inserted,
      skipped_duplicates: skippedDuplicates
    }, 200);

  } catch (err) {
    logDebug_('EXCEPTION', {
      message: err && err.message ? err.message : String(err),
      stack: truncateForCell_(err && err.stack ? err.stack : '', 8000)
    });

    return jsonResponse_({
      ok: false,
      error: String(err && err.message ? err.message : err)
    }, 500);
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}
function doGet(e) {
  return jsonResponse_({
    ok: true,
    message: 'Web App activa',
    tokenRecibido: e && e.parameter ? (e.parameter.token || '') : ''
  }, 200);
}

function normalizePayloadToRows_(payload, receivedAt) {
  const rows = [];
  const receivedAtIso = receivedAt.toISOString();

  if (!payload || typeof payload !== 'object') return rows;

  const metrics =
    payload &&
    payload.data &&
    payload.data.metrics &&
    Array.isArray(payload.data.metrics)
      ? payload.data.metrics
      : null;

  if (metrics && metrics.length) {
    metrics.forEach(metric => {
      const metricName = metric && metric.name ? metric.name : '';
      const units = metric && metric.units ? metric.units : '';

      if (metric && Array.isArray(metric.data) && metric.data.length) {
        metric.data.forEach(item => {
          const row = {
            received_at: receivedAtIso,
            metric_name: metricName,
            units: units
          };
          flattenObjectRecursive_(item, row, '');
          rows.push(row);
        });
      } else {
        rows.push({
          received_at: receivedAtIso,
          metric_name: metricName,
          units: units
        });
      }
    });

    return rows;
  }

  const row = { received_at: receivedAtIso };
  flattenObjectRecursive_(payload, row, '');
  rows.push(row);
  return rows;
}

function normalizeWorkoutPayloadToRows_(payload, receivedAt) {
  const rows = [];
  const receivedAtIso = receivedAt.toISOString();

  if (!payload || typeof payload !== 'object') return rows;

  let workouts = null;

  if (Array.isArray(payload.workouts)) {
    workouts = payload.workouts;
  } else if (payload.data && Array.isArray(payload.data.workouts)) {
    workouts = payload.data.workouts;
  } else if (payload.data && Array.isArray(payload.data)) {
    workouts = payload.data;
  } else if (Array.isArray(payload)) {
    workouts = payload;
  }

  if (workouts && workouts.length) {
    workouts.forEach(item => {
      const row = { received_at: receivedAtIso, dataset: 'workouts' };
      flattenObjectRecursive_(item, row, '');
      rows.push(row);
    });
    return rows;
  }

  const row = { received_at: receivedAtIso, dataset: 'workouts' };
  flattenObjectRecursive_(payload, row, '');
  rows.push(row);
  return rows;
}

function enrichRow_(row) {
  const out = Object.assign({}, row);

  out.metric_name = normalizeMetricName_(out.metric_name);
  out.date_iso = normalizeDateString_(out.date);
  out.received_at = normalizeDateString_(out.received_at) || out.received_at;
  out.qty_num = toNumberOrBlank_(out.qty);
  out.totalSleep_num = toNumberOrBlank_(out.totalSleep);
  out.deep_num = toNumberOrBlank_(out.deep);
  out.rem_num = toNumberOrBlank_(out.rem);
  out.core_num = toNumberOrBlank_(out.core);
  out.awake_num = toNumberOrBlank_(out.awake);
  out.inBed_num = toNumberOrBlank_(out.inBed);
  out.Min_num = toNumberOrBlank_(out.Min);
  out.Max_num = toNumberOrBlank_(out.Max);
  out.Avg_num = toNumberOrBlank_(out.Avg);

  out.dedupe_key = buildDedupeKey_(out);

  return out;
}

function buildDedupeKey_(row) {
  const parts = [
    normalizeMetricName_(row.metric_name),
    normalizeDateString_(row.date) || '',
    String(row.source || '').trim(),
    normalizeNumberForKey_(row.qty),
    normalizeDateString_(row.sleepStart) || '',
    normalizeDateString_(row.sleepEnd) || '',
    normalizeDateString_(row.inBedStart) || '',
    normalizeDateString_(row.inBedEnd) || ''
  ];
  return parts.join('||');
}

function enrichWorkoutRow_(row) {
  const out = Object.assign({}, row);

  out.workout_type = normalizeWorkoutType_(
    out.workout_type ||
    out.type ||
    out.activityType ||
    out.workoutActivityType ||
    out.name
  );

  out.source = String(
    out.source ||
    out['heartRate.avg.source'] ||
    out['heartRate.max.source'] ||
    out['heartRate.min.source'] ||
    out.device ||
    ''
  ).trim();

  out.start_iso = normalizeDateString_(
    out.start || out.startDate || out.start_time || out.begin || out.date
  );
  out.end_iso = normalizeDateString_(
    out.end || out.endDate || out.end_time || ''
  );
  out.date_iso = out.start_iso || normalizeDateString_(out.date);

  out.duration_min = computeWorkoutDurationMinutes_(out);

  out.distance_km = normalizeDistanceKm_(
    out.distance ||
    out.totalDistance ||
    out.distance_km ||
    out.distanceKm ||
    out['distance.qty'] ||
    out['totalDistance.qty'],
    out.distance_units || out['distance.units'] || out['totalDistance.units']
  );

  out.energy_kcal = normalizeEnergyKcal_(
    out.energy ||
    out.totalEnergyBurned ||
    out.totalEnergy ||
    out.activeEnergy ||
    out.activeEnergyBurned ||
    out.kcal ||
    out.calories ||
    out['activeEnergy.qty'] ||
    out['energy.qty'] ||
    out['totalEnergyBurned.qty'] ||
    out['totalEnergy.qty'] ||
    out['activeEnergyBurned.qty'],
    out.energy_units ||
    out.energyUnit ||
    out['energy.units'] ||
    out['activeEnergy.units'] ||
    out['totalEnergyBurned.units'] ||
    out['totalEnergy.units'] ||
    out['activeEnergyBurned.units']
  );
  if (out.energy_kcal === '') {
    out.energy_kcal = computeEnergyFromIntensity_(out);
  }

  out.avg_hr = toNumberOrBlank_(
    out.avg_hr ||
    out.averageHeartRate ||
    out.avgHeartRate ||
    out.heartRateAverage ||
    out['heartRate.avg.qty'] ||
    out['heartRate.avg']
  );

  out.max_hr = toNumberOrBlank_(
    out.max_hr ||
    out.maxHeartRate ||
    out.maximumHeartRate ||
    out['heartRate.max.qty'] ||
    out['heartRate.max']
  );

  out.min_hr = toNumberOrBlank_(
    out.min_hr ||
    out.minimumHeartRate ||
    out['heartRate.min.qty'] ||
    out['heartRate.min']
  );

  delete out.heartRateData;
  delete out.heartRateRecovery;
  Object.keys(out).forEach(k => {
    if (k.startsWith('heartRateData.') || k.startsWith('heartRateRecovery.')) {
      delete out[k];
    }
  });

  out.dedupe_key = buildWorkoutDedupeKey_(out);

  return out;
}

function buildWorkoutDedupeKey_(row) {
  const parts = [
    normalizeWorkoutType_(row.workout_type),
    normalizeDateString_(row.start_iso || row.start || row.startDate || row.date) || '',
    normalizeDateString_(row.end_iso || row.end || row.endDate) || '',
    String(row.source || '').trim(),
    normalizeNumberForKey_(row.duration_min),
    normalizeNumberForKey_(row.distance_km),
    normalizeNumberForKey_(row.energy_kcal)
  ];
  return parts.join('||');
}

function normalizeWorkoutType_(value) {
  return String(value || '').trim();
}

function computeWorkoutDurationMinutes_(row) {
  const direct = toNumberOrBlank_(row.duration_min || row.durationMinutes || row.duration || row.durationInMinutes || row.durationInSeconds);
  if (direct !== '') {
    const units = String(row.durationUnit || row.durationUnits || row.duration_unit || '').toLowerCase().trim();
    if (units.indexOf('sec') !== -1 || units === 's') return direct / 60;
    if (units.indexOf('hour') !== -1 || units === 'h' || units === 'hr' || units === 'hrs') return direct * 60;
    if ((row.duration || row.durationInSeconds) && direct >= 1000) return direct / 60;
    return direct;
  }

  const start = toTimeMs_(row.start || row.startDate || row.start_iso || row.date);
  const end = toTimeMs_(row.end || row.endDate || row.end_iso);
  if (start !== null && end !== null && end >= start) {
    return (end - start) / 60000;
  }
  return '';
}

function normalizeDistanceKm_(value, units) {
  const n = toNumberOrBlank_(value);
  if (n === '') return '';

  const u = String(units || '').toLowerCase().trim();
  if (u.indexOf('km') !== -1) return n;
  if (u === 'mi' || u.indexOf('mile') !== -1) return n * 1.60934;
  if (u === 'm' || u === 'meter' || u === 'meters') return n / 1000;

  return n;
}

function normalizeEnergyKcal_(value, units) {
  if (value === null || value === undefined || value === '') return '';

  if (typeof value === 'object') {
    const qtyValue = value.qty !== undefined ? value.qty : (value.value !== undefined ? value.value : '');
    const qtyUnits = value.units !== undefined ? value.units : (value.unit !== undefined ? value.unit : units);
    return normalizeEnergyKcal_(qtyValue, qtyUnits);
  }

  let parsed = toNumberOrBlank_(value);
  const raw = String(value || '').toLowerCase();

  if (parsed === '') {
    const match = raw.match(/-?\d+(?:[.,]\d+)?/);
    if (match && match[0]) {
      parsed = toNumberOrBlank_(match[0].replace(',', '.'));
    }
  }

  if (parsed === '') return '';

  const combinedUnits = String(units || '').toLowerCase().trim();
  const hasKj = combinedUnits.indexOf('kj') !== -1 || raw.indexOf('kj') !== -1 || raw.indexOf('kilojoule') !== -1;
  if (hasKj) return parsed / 4.184;

  return parsed;
}

function computeEnergyFromIntensity_(row) {
  const durationMin = toNumberOrBlank_(row.duration_min);
  if (durationMin === '' || durationMin <= 0) return '';

  const intensityQty = toNumberOrBlank_(
    row['intensity.qty'] ||
    row.intensity_qty ||
    row.intensityQty ||
    row.intensity
  );
  if (intensityQty === '' || intensityQty <= 0) return '';

  const units = String(
    row['intensity.units'] ||
    row.intensity_units ||
    row.intensityUnit ||
    ''
  ).toLowerCase().trim();
  const compactUnits = units.replace(/\s+/g, '');
  const weightKg = toNumberOrBlank_(
    row.weight_kg ||
    row.weightKg ||
    row['bodyMass.qty'] ||
    row.body_weight_kg
  );

  const looksPerMinute = units.indexOf('/min') !== -1 || units.indexOf('per min') !== -1 || units.indexOf('min-1') !== -1;
  const looksPerHour = units.indexOf('/hr') !== -1 || units.indexOf('/hour') !== -1 || units.indexOf('per hour') !== -1 || units.indexOf('h-1') !== -1;
  const looksPerKg = compactUnits.indexOf('/kg') !== -1 || compactUnits.indexOf('·kg') !== -1 || compactUnits.indexOf('*kg') !== -1 || compactUnits.indexOf('kg-1') !== -1;
  const looksKcal = units.indexOf('kcal') !== -1 || units.indexOf('kilocal') !== -1 || units.indexOf('cal') === 0;

  if (looksPerHour && looksPerKg && looksKcal) {
    if (weightKg === '' || weightKg <= 0) return '';
    return intensityQty * weightKg * (durationMin / 60);
  }

  if (looksPerMinute && looksKcal) {
    return intensityQty * durationMin;
  }

  const looksKj = units.indexOf('kj') !== -1 || units.indexOf('kilojoule') !== -1;
  if (looksPerHour && looksPerKg && looksKj) {
    if (weightKg === '' || weightKg <= 0) return '';
    return (intensityQty * weightKg * (durationMin / 60)) / 4.184;
  }

  if (looksPerMinute && looksKj) {
    return (intensityQty * durationMin) / 4.184;
  }

  const looksMet = units.indexOf('met') !== -1;
  if (looksMet) {
    if (weightKg === '' || weightKg <= 0) return '';
    return (intensityQty * 3.5 * weightKg / 200) * durationMin;
  }

  return '';
}

function enrichWorkoutEnergyWithFallbackWeight_(row, fallbackWeightKg) {
  if (!row || row.energy_kcal !== '') return row;
  if (fallbackWeightKg === '' || fallbackWeightKg === null || fallbackWeightKg === undefined) return row;

  const out = Object.assign({}, row);
  if (toNumberOrBlank_(out.weight_kg) === '') {
    out.weight_kg = fallbackWeightKg;
  }
  out.energy_kcal = computeEnergyFromIntensity_(out);
  return out;
}

function getLatestWeightKgFromRaw_(ss) {
  const rawSheet = ss.getSheetByName(SHEET_NAME);
  if (!rawSheet || rawSheet.getLastRow() < 2) return '';

  const data = rawSheet.getDataRange().getValues();
  const header = data[0];
  const metricIdx = header.indexOf('metric_name');
  const qtyIdx = header.indexOf('qty');
  const qtyNumIdx = header.indexOf('qty_num');
  if (metricIdx === -1 || (qtyIdx === -1 && qtyNumIdx === -1)) return '';

  for (let i = data.length - 1; i >= 1; i--) {
    const metricName = normalizeMetricName_(data[i][metricIdx]);
    if (metricName !== 'weight_body_mass') continue;

    const qtyValue = qtyNumIdx !== -1 ? data[i][qtyNumIdx] : data[i][qtyIdx];
    const parsed = toNumberOrBlank_(qtyValue);
    if (parsed !== '' && parsed > 0) return parsed;
  }
  return '';
}

function recalculateExistingWorkoutEnergyKcal_(ss) {
  const sheet = ss.getSheetByName(WORKOUTS_RAW_SHEET_NAME);
  if (!sheet || sheet.getLastRow() < 2) return 0;

  const data = sheet.getDataRange().getValues();
  const header = data[0];
  const energyIdx = header.indexOf('energy_kcal');
  if (energyIdx === -1) return 0;

  const fallbackWeightKg = getLatestWeightKgFromRaw_(ss);
  if (fallbackWeightKg === '') return 0;

  let updated = 0;
  const newEnergyCol = data.slice(1).map(row => {
    const current = row[energyIdx];
    if (toNumberOrBlank_(current) !== '') return [current];

    const obj = rowArrayToObject_(header, row);
    const enriched = enrichWorkoutRow_(obj);
    const withWeight = enrichWorkoutEnergyWithFallbackWeight_(enriched, fallbackWeightKg);
    const next = withWeight.energy_kcal;

    if (toNumberOrBlank_(next) !== '') {
      updated++;
      return [next];
    }
    return [current];
  });

  if (updated > 0) {
    sheet.getRange(2, energyIdx + 1, newEnergyCol.length, 1).setValues(newEnergyCol);
  }
  return updated;
}

function getExistingDedupeKeys_(sheet) {
  const map = {};
  if (sheet.getLastRow() < 2) return map;

  const header = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const idx = header.indexOf('dedupe_key');
  if (idx === -1) return map;

  const values = sheet.getRange(2, idx + 1, sheet.getLastRow() - 1, 1).getValues();
  values.forEach(r => {
    const key = String(r[0] || '');
    if (key) map[key] = true;
  });
  return map;
}

function rebuildWorkoutsSheet_(ss) {
  const rawSheet = ss.getSheetByName(WORKOUTS_RAW_SHEET_NAME);
  const target = getOrCreateSheet_(ss, WORKOUTS_SHEET_NAME);
  target.clearContents();

  if (!rawSheet || rawSheet.getLastRow() < 2) {
    target.getRange(1, 1).setValue('Sin datos todavía');
    return;
  }

  const data = rawSheet.getDataRange().getValues();
  const header = data[0];
  const rows = data.slice(1);

  const WORKOUTS_COLUMN_WHITELIST = [
    'received_at',
    'dataset',
    'workout_type',
    'source',
    'start_iso',
    'end_iso',
    'date_iso',
    'duration_min',
    'distance_km',
    'energy_kcal',
    'avg_hr',
    'min_hr',
    'max_hr'
  ].filter(col => header.includes(col));

  const columnIndexes = WORKOUTS_COLUMN_WHITELIST.map(col => header.indexOf(col));
  const slimRows = rows.map(r => columnIndexes.map(idx => r[idx]));
  const displayRows = slimRows.map(r => formatRowForDisplay_(WORKOUTS_COLUMN_WHITELIST, r));

  target.getRange(1, 1, 1, WORKOUTS_COLUMN_WHITELIST.length).setValues([WORKOUTS_COLUMN_WHITELIST]);
  if (displayRows.length) {
    target.getRange(2, 1, displayRows.length, WORKOUTS_COLUMN_WHITELIST.length).setValues(displayRows);
  }
  formatDataSheet_(target, WORKOUTS_COLUMN_WHITELIST, displayRows.length);
  autoResizeSomeColumns_(target, Math.min(WORKOUTS_COLUMN_WHITELIST.length, 16));
}

function rebuildDerivedSheets_(ss) {
  const rawSheet = ss.getSheetByName(SHEET_NAME);
  if (!rawSheet || rawSheet.getLastRow() < 2) return;

  const data = rawSheet.getDataRange().getValues();
  const header = data[0];
  const rows = data.slice(1);

  const metricIdx = header.indexOf('metric_name');
  if (metricIdx === -1) return;

  const SHEET_COLUMN_WHITELISTS = {
    peso: ['received_at', 'metric_name', 'units', 'qty', 'date', 'source', 'date_iso', 'qty_num'],
    cardio: ['received_at', 'metric_name', 'units', 'qty', 'date', 'source', 'Min', 'Max', 'Avg', 'date_iso', 'qty_num', 'Min_num', 'Max_num', 'Avg_num'],
    sueno: ['received_at', 'metric_name', 'units', 'date', 'source', 'sleepEnd', 'inBed', 'core', 'rem', 'awake', 'sleepStart', 'inBedStart', 'deep', 'totalSleep', 'inBedEnd', 'asleep', 'date_iso', 'totalSleep_num', 'deep_num', 'rem_num', 'core_num', 'awake_num', 'inBed_num']
  };

  Object.keys(DERIVED_SHEETS).forEach(sheetName => {
    const allowedMetrics = DERIVED_SHEETS[sheetName];
    const filtered = rows.filter(r => allowedMetrics.includes(normalizeMetricName_(r[metricIdx])));

    const target = getOrCreateSheet_(ss, sheetName);
    target.clearContents();

    const allowedColumns = (SHEET_COLUMN_WHITELISTS[sheetName] || header).filter(col => header.includes(col));
    const columnIndexes = allowedColumns.map(col => header.indexOf(col));

    target.getRange(1, 1, 1, allowedColumns.length).setValues([allowedColumns]);

    if (filtered.length) {
      const slimRows = filtered.map(r => columnIndexes.map(idx => r[idx]));
      const displayRows = slimRows.map(r => formatRowForDisplay_(allowedColumns, r));
      target.getRange(2, 1, displayRows.length, allowedColumns.length).setValues(displayRows);
      formatDataSheet_(target, allowedColumns, displayRows.length);
      autoResizeSomeColumns_(target, Math.min(allowedColumns.length, 16));
    } else {
      formatDataSheet_(target, allowedColumns, 0);
    }
  });
}

function rebuildDashboard_(ss) {
  const dashboard = getOrCreateSheet_(ss, DASHBOARD_SHEET_NAME);
  dashboard.clearContents();

  const rawSheet = ss.getSheetByName(SHEET_NAME);
  if (!rawSheet || rawSheet.getLastRow() < 2) {
    dashboard.getRange(1, 1).setValue('Sin datos todavía');
    return;
  }

  const data = rawSheet.getDataRange().getValues();
  const header = data[0];
  const rows = data.slice(1).map(r => rowArrayToObject_(header, r));

  const workoutsSheet = ss.getSheetByName(WORKOUTS_RAW_SHEET_NAME);
  let workoutRows = [];
  if (workoutsSheet && workoutsSheet.getLastRow() >= 2) {
    const wData = workoutsSheet.getDataRange().getValues();
    const wHeader = wData[0];
    workoutRows = wData.slice(1).map(r => rowArrayToObject_(wHeader, r));
  }

  const now = new Date();
  const cutoff7d = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const cutoff14d = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
  const cutoff15d = new Date(now.getTime() - 15 * 24 * 60 * 60 * 1000);

  const metrics = {
    ultimo_peso: getLatestQty_(rows, 'weight_body_mass'),
    peso_medio_7d: getAverageQtySince_(rows, 'weight_body_mass', cutoff7d),
    ultimo_vo2_max: getLatestQty_(rows, 'vo2_max'),
    resting_hr_media_7d: getAverageQtySince_(rows, 'resting_heart_rate', cutoff7d),
    walking_hr_media_7d: getAverageQtySince_(rows, 'walking_heart_rate_average', cutoff7d),
    sueno_medio_7d_horas: getAverageFieldSince_(rows, 'sleep_analysis', 'totalSleep_num', cutoff7d),
    ultimo_sueno_horas: getLatestField_(rows, 'sleep_analysis', 'totalSleep_num'),
    ultima_fecha_import: getLatestReceivedAt_(rows)
  };

  const energy = computeEnergyReadiness_(rows, workoutRows, now, cutoff14d);

  const cards = [
    ['Último peso (kg)', formatMetricValue_(metrics.ultimo_peso, 2)],
    ['Peso medio 7 días (kg)', formatMetricValue_(metrics.peso_medio_7d, 2)],
    ['Último VO2 max', formatMetricValue_(metrics.ultimo_vo2_max, 2)],
    ['Resting HR media 7 días', formatMetricValue_(metrics.resting_hr_media_7d, 2)],
    ['Walking HR media 7 días', formatMetricValue_(metrics.walking_hr_media_7d, 2)],
    ['Sueño medio 7 días (h)', formatMetricValue_(metrics.sueno_medio_7d_horas, 2)],
    ['Último sueño total (h)', formatMetricValue_(metrics.ultimo_sueno_horas, 2)],
    ['Última importación', formatDateForDisplay_(metrics.ultima_fecha_import)]
  ];

  const DASHBOARD_COLS = 10;
  dashboard.getRange(1, 1, 1, DASHBOARD_COLS).merge();
  dashboard.getRange(1, 1).setValue('Dashboard de Salud');
  dashboard.getRange(2, 1, 1, DASHBOARD_COLS).merge();
  dashboard.getRange(2, 1).setValue('Resumen automático generado desde raw');

  let rowCursor = 4;
  writeEnergyHero_(dashboard, energy, rowCursor, 1);
  rowCursor += getEnergyHeroHeight_() + 1;

  writeDashboardCards_(dashboard, rowCursor, 1, cards, 2, 5);
  rowCursor += getDashboardCardsHeight_(cards.length, 2) + 1;

  writeWeightChartSection_(dashboard, rows, cutoff15d, rowCursor, 1);
  rowCursor += getWeightChartSectionHeight_() + 2;

  const sections = buildDashboardDetailSections_(rows, cutoff7d);
  sections.forEach(section => {
    dashboard.getRange(rowCursor, 1, section.length, section[0].length).setValues(section);
    rowCursor += section.length + 2;
  });

  formatDashboard_(dashboard, cards.length, sections, energy);
}

function buildDashboardDetailSections_(rows, cutoff7d) {
  return [
    buildLatestRowsSection_(rows, 'Últimos pesos', 'weight_body_mass', ['date_iso', 'qty_num', 'source'], 10),
    buildLatestRowsSection_(rows, 'Últimos VO2 max', 'vo2_max', ['date_iso', 'qty_num', 'source'], 10),
    buildLatestRowsSection_(rows, 'Últimos resting HR', 'resting_heart_rate', ['date_iso', 'qty_num', 'source'], 10),
    buildLatestRowsSection_(rows, 'Últimos walking HR', 'walking_heart_rate_average', ['date_iso', 'qty_num', 'source'], 10),
    buildLatestRowsSection_(rows, 'Últimos registros sueño', 'sleep_analysis', ['date_iso', 'totalSleep_num', 'deep_num', 'rem_num', 'core_num', 'awake_num'], 10)
  ];
}

function buildLatestRowsSection_(rows, title, metricName, fields, limit) {
  const filtered = rows
    .filter(r => normalizeMetricName_(r.metric_name) === metricName)
    .filter(r => r.date_iso)
    .sort((a, b) => (toTimeMs_(b.date_iso) || -Infinity) - (toTimeMs_(a.date_iso) || -Infinity))
    .slice(0, limit);

  const width = fields.length;
  const out = [];

  out.push([title].concat(Array(Math.max(0, width - 1)).fill('')));
  out.push(fields);

  filtered.forEach(r => {
    out.push(fields.map(f => formatFieldForDisplay_(f, r[f])));
  });

  if (filtered.length === 0) {
    const emptyRow = Array(width).fill('');
    emptyRow[0] = 'Sin datos';
    out.push(emptyRow);
  }

  return out;
}

function writeEnergyHero_(sheet, energy, startRow, startCol) {
  sheet.getRange(startRow, startCol, 1, 10).merge();
  sheet.getRange(startRow + 1, startCol, 2, 3).merge();
  sheet.getRange(startRow + 1, startCol + 3, 2, 7).merge();

  sheet.getRange(startRow, startCol).setValue('Estado de energía estimado para hoy');
  sheet.getRange(startRow + 1, startCol).setValue(String(energy.score));
  sheet.getRange(startRow + 1, startCol + 3).setValue(energy.status + ' — ' + energy.message);

  const detailHeader = ['Sueño', 'Recuperación', 'Carga reciente', 'Base fisiológica'];
  const detailValues = [
    formatMetricValue_(energy.sleepScore, 0),
    formatMetricValue_(energy.recoveryScore, 0),
    formatMetricValue_(energy.loadScore, 0),
    formatMetricValue_(energy.capacityScore, 0)
  ];

  sheet.getRange(startRow + 3, startCol, 1, 4).setValues([detailHeader]);
  sheet.getRange(startRow + 4, startCol, 1, 4).setValues([detailValues]);
}

function getEnergyHeroHeight_() {
  return 5;
}

function writeDashboardCards_(sheet, startRow, startCol, cards, cardsPerRow, cardWidth) {
  cards.forEach((card, idx) => {
    const rowBlock = Math.floor(idx / cardsPerRow);
    const colBlock = idx % cardsPerRow;
    const row = startRow + rowBlock * 3;
    const col = startCol + colBlock * cardWidth;

    sheet.getRange(row, col, 1, 4).merge();
    sheet.getRange(row + 1, col, 1, 4).merge();

    sheet.getRange(row, col).setValue(card[0]);
    sheet.getRange(row + 1, col).setValue(card[1] === '' ? '—' : card[1]);
  });
}

function writeWeightChartSection_(sheet, rows, cutoffDate, startRow, startCol) {
  const weightRows = rows
    .filter(r => normalizeMetricName_(r.metric_name) === 'weight_body_mass')
    .filter(r => isOnOrAfter_(r.date_iso, cutoffDate))
    .map(r => ({
      date_iso: r.date_iso,
      qty_num: toNumberOrBlank_(r.qty_num !== '' ? r.qty_num : r.qty)
    }))
    .filter(r => r.date_iso && r.qty_num !== '')
    .sort((a, b) => (toTimeMs_(a.date_iso) || Infinity) - (toTimeMs_(b.date_iso) || Infinity));

  const sectionHeight = getWeightChartSectionHeight_();
  const sectionWidth = 10;
  clearMergedRangesInArea_(sheet, startRow, startCol, sectionHeight, sectionWidth);

  sheet.getRange(startRow, startCol, 1, sectionWidth).merge();
  sheet.getRange(startRow, startCol).setValue('Seguimiento del peso — últimos 15 días');

  const headerRow = startRow + 1;
  sheet.getRange(headerRow, startCol, 1, 3).setValues([['Fecha', 'Peso (kg)', 'Tendencia']]);

  const existingCharts = sheet.getCharts();
  existingCharts.forEach(c => {
    const info = c.getContainerInfo();
    if (info && info.getAnchorRow() >= startRow && info.getAnchorRow() <= startRow + sectionHeight) {
      sheet.removeChart(c);
    }
  });

  if (weightRows.length) {
    const trendValues = buildWeightTrendSeries_(weightRows.map(r => r.qty_num));
    const values = weightRows.map((r, i) => [formatDateShortForChart_(r.date_iso), r.qty_num, trendValues[i]]);
    sheet.getRange(headerRow + 1, startCol, values.length, 3).setValues(values);

    const minWeight = Math.min.apply(null, weightRows.map(r => r.qty_num));
    const maxWeight = Math.max.apply(null, weightRows.map(r => r.qty_num));
    const rangePadding = Math.max(0.2, (maxWeight - minWeight) * 0.35 || 0.2);

    const chart = sheet.newChart()
      .asLineChart()
      .addRange(sheet.getRange(headerRow, startCol, values.length + 1, 3))
      .setPosition(startRow + 1, startCol + 4, 0, 0)
      .setOption('title', 'Evolución del peso')
      .setOption('subtitle', 'Serie diaria y tendencia suavizada')
      .setOption('legend', { position: 'bottom' })
      .setOption('curveType', 'function')
      .setOption('pointSize', 7)
      .setOption('lineWidth', 3)
      .setOption('series', {
        0: { targetAxisIndex: 0 },
        1: { targetAxisIndex: 0, lineDashStyle: [6, 4], pointSize: 0 }
      })
      .setOption('hAxis', { title: 'Fecha', slantedText: true, slantedTextAngle: 35 })
      .setOption('vAxis', {
        title: 'kg',
        viewWindow: {
          min: minWeight - rangePadding,
          max: maxWeight + rangePadding
        }
      })
      .build();

    sheet.insertChart(chart);
  } else {
    sheet.getRange(headerRow + 1, startCol).setValue('Sin datos');
  }
}

function clearMergedRangesInArea_(sheet, startRow, startCol, numRows, numCols) {
  const safeRows = Math.max(1, numRows);
  const safeCols = Math.max(1, numCols);
  const area = sheet.getRange(startRow, startCol, safeRows, safeCols);
  const mergedRanges = area.getMergedRanges();
  mergedRanges.forEach(range => range.breakApart());
}

function getWeightChartSectionHeight_() {
  return 18;
}

function buildWeightTrendSeries_(values) {
  const out = [];
  for (let i = 0; i < values.length; i++) {
    const start = Math.max(0, i - 2);
    const slice = values.slice(start, i + 1);
    const avg = slice.reduce((a, b) => a + b, 0) / slice.length;
    out.push(Number(avg.toFixed(2)));
  }
  return out;
}

function formatFieldForDisplay_(field, value) {
  if (value === '' || value === null || value === undefined) return '';

  if (DATE_FIELDS.has(field)) {
    return formatDateForDisplay_(value);
  }

  if (DECIMAL2_FIELDS.has(field)) {
    const n = toNumberOrBlank_(value);
    return n === '' ? '' : formatMetricValue_(n, 2);
  }

  if (field === 'qty_num') {
    const n = toNumberOrBlank_(value);
    return n === '' ? '' : n;
  }

  return value;
}

function formatDateForDisplay_(value) {
  if (!value) return '';
  const d = new Date(value);
  if (isNaN(d.getTime())) return String(value);

  const dd = pad2_(d.getDate());
  const mm = pad2_(d.getMonth() + 1);
  const yyyy = d.getFullYear();
  const hh = pad2_(d.getHours());
  const min = pad2_(d.getMinutes());

  return hh + ':' + min + ' ' + dd + '-' + mm + '-' + yyyy;
}

function formatDateShortForChart_(value) {
  if (!value) return '';
  const d = new Date(value);
  if (isNaN(d.getTime())) return String(value);

  const dd = pad2_(d.getDate());
  const mm = pad2_(d.getMonth() + 1);
  return dd + '-' + mm;
}

function pad2_(n) {
  return String(n).padStart(2, '0');
}

function formatDashboard_(dashboard, cardCount, sections, energy) {
  try {
    dashboard.setFrozenRows(2);
  } catch (e) {}

  try {
    dashboard.getRange(1, 1, 1, 10)
      .setFontSize(18)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setBackground('#d9ead3');

    dashboard.getRange(2, 1, 1, 10)
      .setFontSize(10)
      .setFontColor('#444444')
      .setHorizontalAlignment('center')
      .setBackground('#f3f3f3');

    const energyStartRow = 4;
    dashboard.getRange(energyStartRow, 1, 1, 10)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setBackground('#d9ead3');

    dashboard.getRange(energyStartRow + 1, 1, 2, 3)
      .setFontSize(28)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBackground(energy.color)
      .setBorder(true, true, true, true, true, true);

    dashboard.getRange(energyStartRow + 1, 4, 2, 7)
      .setFontSize(16)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setWrap(true)
      .setBackground(energy.color)
      .setBorder(true, true, true, true, true, true);

    dashboard.getRange(energyStartRow + 3, 1, 1, 4)
      .setFontWeight('bold')
      .setBackground('#fce5cd')
      .setHorizontalAlignment('center');

    dashboard.getRange(energyStartRow + 4, 1, 1, 4)
      .setFontSize(12)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setBackground('#fff2cc')
      .setBorder(true, true, true, true, true, true);
  } catch (e) {}

  const cardsStartRow = 4 + getEnergyHeroHeight_() + 1;
  const cardsPerRow = 2;
  const cardWidth = 5;
  for (let idx = 0; idx < cardCount; idx++) {
    const rowBlock = Math.floor(idx / cardsPerRow);
    const colBlock = idx % cardsPerRow;
    const row = cardsStartRow + rowBlock * 3;
    const col = 1 + colBlock * cardWidth;

    try {
      dashboard.getRange(row, col, 1, 4)
        .setFontWeight('bold')
        .setFontSize(10)
        .setHorizontalAlignment('center')
        .setVerticalAlignment('middle')
        .setBackground('#cfe2f3')
        .setBorder(true, true, false, true, true, true);

      dashboard.getRange(row + 1, col, 1, 4)
        .setFontSize(16)
        .setFontWeight('bold')
        .setHorizontalAlignment('center')
        .setVerticalAlignment('middle')
        .setBackground('#ffffff')
        .setBorder(false, true, true, true, true, true);
    } catch (e) {}
  }

  const chartStartRow = cardsStartRow + getDashboardCardsHeight_(cardCount, cardsPerRow) + 1;
  try {
    dashboard.getRange(chartStartRow, 1, 1, 10)
      .setFontWeight('bold')
      .setBackground('#d9ead3')
      .setHorizontalAlignment('left');

    dashboard.getRange(chartStartRow + 1, 1, 1, 3)
      .setFontWeight('bold')
      .setBackground('#fce5cd')
      .setHorizontalAlignment('center');
  } catch (e) {}

  let startRow = chartStartRow + getWeightChartSectionHeight_() + 2;
  sections.forEach(section => {
    const width = section[0].length;
    try {
      dashboard.getRange(startRow, 1, 1, width)
        .setFontWeight('bold')
        .setBackground('#ead1dc')
        .setHorizontalAlignment('left');

      dashboard.getRange(startRow + 1, 1, 1, width)
        .setFontWeight('bold')
        .setBackground('#fce5cd')
        .setHorizontalAlignment('center');

      if (section.length > 2) {
        dashboard.getRange(startRow + 2, 1, section.length - 2, width)
          .setBackground('#ffffff')
          .setBorder(true, true, true, true, true, true);
      }
    } catch (e) {}
    startRow += section.length + 2;
  });

  autoResizeSomeColumns_(dashboard, 10);
  try {
    dashboard.setColumnWidths(1, 10, 125);
  } catch (e) {}
}

function getDashboardCardsHeight_(cardCount, cardsPerRow) {
  return Math.ceil(cardCount / cardsPerRow) * 3;
}

function computeEnergyReadiness_(rows, workoutRows, now, cutoff14d) {
  const sleepScore = computeSleepScore_(rows, cutoff14d);
  const recoveryScore = computeRecoveryScore_(rows, cutoff14d);
  const loadScore = computeLoadScore_(workoutRows, now);
  const capacityScore = computeCapacityScore_(rows, cutoff14d);

  const score = Math.round(
    0.50 * sleepScore +
    0.30 * recoveryScore +
    0.15 * loadScore +
    0.05 * capacityScore
  );

  return {
    score: score,
    status: energyStatusFromScore_(score),
    color: energyColorFromScore_(score),
    message: energyMessageFromScore_(score),
    sleepScore: sleepScore,
    recoveryScore: recoveryScore,
    loadScore: loadScore,
    capacityScore: capacityScore
  };
}

function computeSleepScore_(rows, cutoff14d) {
  const latest = getLatestRow_(rows, 'sleep_analysis');
  if (!latest) return 50;

  const totalSleep = toNumberOrBlank_(latest.totalSleep_num !== '' ? latest.totalSleep_num : latest.totalSleep);
  const awake = toNumberOrBlank_(latest.awake_num !== '' ? latest.awake_num : latest.awake);
  const deep = toNumberOrBlank_(latest.deep_num !== '' ? latest.deep_num : latest.deep);
  const rem = toNumberOrBlank_(latest.rem_num !== '' ? latest.rem_num : latest.rem);

  let durationScore = 40;
  if (totalSleep >= 7.5 && totalSleep <= 8.5) durationScore = 100;
  else if (totalSleep >= 7) durationScore = 80;
  else if (totalSleep >= 6.5) durationScore = 60;
  else if (totalSleep >= 6) durationScore = 40;
  else durationScore = 20;

  let continuityScore = 70;
  if (awake !== '') {
    if (awake <= 0.2) continuityScore = 100;
    else if (awake <= 0.4) continuityScore = 80;
    else if (awake <= 0.7) continuityScore = 60;
    else if (awake <= 1.0) continuityScore = 40;
    else continuityScore = 20;
  }

  let architectureScore = 60;
  if (totalSleep > 0 && deep !== '' && rem !== '') {
    const restorativePct = (deep + rem) / totalSleep;
    if (restorativePct >= 0.35) architectureScore = 100;
    else if (restorativePct >= 0.30) architectureScore = 80;
    else if (restorativePct >= 0.25) architectureScore = 60;
    else architectureScore = 40;
  }

  let regularityScore = 70;
  const sleepRows = rows
    .filter(r => normalizeMetricName_(r.metric_name) === 'sleep_analysis')
    .filter(r => isOnOrAfter_(r.date_iso, cutoff14d))
    .filter(r => r.sleepStart)
    .sort((a, b) => (toTimeMs_(b.date_iso) || -Infinity) - (toTimeMs_(a.date_iso) || -Infinity));

  if (latest.sleepStart && sleepRows.length >= 4) {
    const minutes = sleepRows.map(r => minutesOfDay_(r.sleepStart)).filter(v => v !== '');
    const baseline = average_(minutes);
    const current = minutesOfDay_(latest.sleepStart);
    if (baseline !== '' && current !== '') {
      const diff = Math.abs(current - baseline);
      if (diff <= 30) regularityScore = 100;
      else if (diff <= 60) regularityScore = 80;
      else if (diff <= 90) regularityScore = 60;
      else if (diff <= 120) regularityScore = 40;
      else regularityScore = 20;
    }
  }

  return Math.round(0.45 * durationScore + 0.25 * regularityScore + 0.20 * continuityScore + 0.10 * architectureScore);
}

function computeRecoveryScore_(rows, cutoff14d) {
  const latestRhr = getLatestQty_(rows, 'resting_heart_rate');
  const rhrValues = rows
    .filter(r => normalizeMetricName_(r.metric_name) === 'resting_heart_rate')
    .filter(r => isOnOrAfter_(r.date_iso, cutoff14d))
    .map(r => toNumberOrBlank_(r.qty_num !== '' ? r.qty_num : r.qty))
    .filter(v => v !== '');

  let rhrScore = 70;
  if (latestRhr !== '' && rhrValues.length >= 4) {
    const baseline = average_(rhrValues);
    const diff = latestRhr - baseline;
    if (diff <= -1) rhrScore = 100;
    else if (diff <= 1) rhrScore = 90;
    else if (diff <= 2) rhrScore = 75;
    else if (diff <= 4) rhrScore = 50;
    else rhrScore = 20;
  }

  const latestWalking = getLatestQty_(rows, 'walking_heart_rate_average');
  const walkingValues = rows
    .filter(r => normalizeMetricName_(r.metric_name) === 'walking_heart_rate_average')
    .filter(r => isOnOrAfter_(r.date_iso, cutoff14d))
    .map(r => toNumberOrBlank_(r.qty_num !== '' ? r.qty_num : r.qty))
    .filter(v => v !== '');

  let walkingScore = 70;
  if (latestWalking !== '' && walkingValues.length >= 4) {
    const baselineW = average_(walkingValues);
    const diffPct = baselineW ? ((latestWalking - baselineW) / baselineW) * 100 : 0;
    if (diffPct <= -3) walkingScore = 95;
    else if (diffPct <= 2) walkingScore = 85;
    else if (diffPct <= 5) walkingScore = 70;
    else if (diffPct <= 8) walkingScore = 50;
    else walkingScore = 30;
  }

  return Math.round(0.75 * rhrScore + 0.25 * walkingScore);
}

function computeLoadScore_(workoutRows, now) {
  if (!workoutRows || !workoutRows.length) return 100;

  const last24h = workoutRows.filter(r => {
    const start = toTimeMs_(r.start_iso || r.date_iso || r.start || r.startDate);
    return start && start >= (now.getTime() - 24 * 60 * 60 * 1000);
  });

  if (!last24h.length) return 100;

  const load = last24h.reduce((sum, r) => {
    const dur = toNumberOrBlank_(r.duration_min);
    const kcal = toNumberOrBlank_(r.energy_kcal);
    const hr = toNumberOrBlank_(r.avg_hr);
    const hrFactor = hr === '' ? 1 : (hr >= 150 ? 1.5 : hr >= 130 ? 1.25 : 1.0);
    return sum + ((dur === '' ? 0 : dur) * hrFactor) + ((kcal === '' ? 0 : kcal / 8));
  }, 0);

  if (load < 45) return 90;
  if (load < 90) return 75;
  if (load < 140) return 60;
  if (load < 220) return 40;
  return 20;
}

function computeCapacityScore_(rows, cutoff14d) {
  const latestVo2 = getLatestQty_(rows, 'vo2_max');
  const vo2Values = rows
    .filter(r => normalizeMetricName_(r.metric_name) === 'vo2_max')
    .filter(r => isOnOrAfter_(r.date_iso, cutoff14d))
    .map(r => toNumberOrBlank_(r.qty_num !== '' ? r.qty_num : r.qty))
    .filter(v => v !== '');

  if (latestVo2 === '' || vo2Values.length < 2) return 70;

  const baseline = average_(vo2Values);
  const diff = latestVo2 - baseline;
  if (diff >= 0.3) return 100;
  if (diff >= -0.1) return 85;
  if (diff >= -0.5) return 65;
  return 45;
}

function energyStatusFromScore_(score) {
  if (score >= 80) return 'Alta energía';
  if (score >= 65) return 'Buena energía';
  if (score >= 50) return 'Energía media';
  if (score >= 35) return 'Energía baja';
  return 'Muy baja energía';
}

function energyMessageFromScore_(score) {
  if (score >= 80) return 'Buen día para apretar';
  if (score >= 65) return 'Día sólido y aprovechable';
  if (score >= 50) return 'Mejor rendir con cabeza';
  if (score >= 35) return 'Conviene regular y no forzar';
  return 'Prioriza recuperación';
}

function energyColorFromScore_(score) {
  if (score >= 80) return '#93c47d';
  if (score >= 65) return '#b6d7a8';
  if (score >= 50) return '#ffe599';
  if (score >= 35) return '#f6b26b';
  return '#e06666';
}

function minutesOfDay_(value) {
  if (!value) return '';
  const d = new Date(value);
  if (isNaN(d.getTime())) return '';
  return d.getHours() * 60 + d.getMinutes();
}

function rebuildStatsSheet_(ss) {
  const stats = getOrCreateSheet_(ss, STATS_SHEET_NAME);
  stats.clearContents();

  const sheets = ss.getSheets();
  const maxCells = 10000000;
  let totalUsedCells = 0;

  const rows = [['Pestaña', 'Filas usadas', 'Columnas usadas', 'Celdas usadas']];

  sheets.forEach(sheet => {
    const name = sheet.getName();
    const lastRow = sheet.getLastRow();
    const lastColumn = sheet.getLastColumn();
    const usedCells = lastRow * lastColumn;

    rows.push([name, lastRow, lastColumn, usedCells]);
    totalUsedCells += usedCells;
  });

  const remaining = Math.max(0, maxCells - totalUsedCells);
  const usagePct = maxCells ? totalUsedCells / maxCells : 0;

  rows.push(['', '', '', '']);
  rows.push(['TOTAL', '', '', totalUsedCells]);
  rows.push(['LÍMITE GOOGLE SHEETS', '', '', maxCells]);
  rows.push(['RESTANTE', '', '', remaining]);
  rows.push(['% USADO', '', '', usagePct]);

  stats.getRange(1, 1, rows.length, rows[0].length).setValues(rows);

  try {
    stats.setFrozenRows(1);
    stats.getRange(1, 1, 1, 4)
      .setFontWeight('bold')
      .setBackground('#d9ead3');

    const summaryStart = rows.length - 3;
    stats.getRange(summaryStart, 1, 4, 4)
      .setFontWeight('bold')
      .setBackground('#f4cccc');

    stats.getRange(rows.length, 4).setNumberFormat('0.00%');
    autoResizeSomeColumns_(stats, 4);
  } catch (e) {}

  try {
    ss.setActiveSheet(stats);
    ss.moveActiveSheet(ss.getNumSheets());
  } catch (e) {}
}

function rebuildReadiness50dSheet_(ss) {
  const sheet = getOrCreateSheet_(ss, READINESS_SHEET_NAME);
  const existingManual = getExistingReadinessManualMap_(sheet);
  sheet.clearContents();

  const rawRows = getRowsAsObjects_(ss.getSheetByName(SHEET_NAME));
  const workoutRows = getRowsAsObjects_(ss.getSheetByName(WORKOUTS_RAW_SHEET_NAME));
  const grouped = {};

  rawRows.forEach(row => {
    const metric = normalizeMetricName_(row.metric_name);
    const dayKey = dayKeyFromDateValue_(row.date_iso || row.date || row.received_at);
    if (!dayKey) return;
    if (!grouped[dayKey]) grouped[dayKey] = {};
    const day = grouped[dayKey];

    if (metric === 'sleep_analysis') {
      assignIfNotBlank_(day, 'sleep_hours', toNumberOrBlank_(row.totalSleep_num !== '' ? row.totalSleep_num : row.totalSleep));
      assignIfNotBlank_(day, 'sleep_quality', toNumberOrBlank_(row.sleep_quality || row.sleepScore || row.qualityScore));
    }
    if (metric === 'resting_heart_rate') {
      assignIfNotBlank_(day, 'hr_resting', toNumberOrBlank_(row.qty_num !== '' ? row.qty_num : row.qty));
    }
    if (metric === 'heart_rate_variability_sdnn') {
      const hrv = extractHrvValue_(row);
      if (hrv !== '') {
        if (!day.hrv_values) day.hrv_values = [];
        day.hrv_values.push(hrv);
      }
    }
    if (metric === 'weight_body_mass') {
      assignIfNotBlank_(day, 'peso_kg', toNumberOrBlank_(row.qty_num !== '' ? row.qty_num : row.qty));
    }
    assignIfNotBlank_(day, 'readiness_score', toNumberOrBlank_(row.readiness_score || row.readinessScore));
    assignIfNotBlank_(day, 'fuente', String(row.source || '').trim());
    assignIfNotBlank_(day, 'ultima_actualizacion', row.received_at || row.date_iso || row.date);
  });

  const today = new Date();

  for (let i = 0; i < 50; i++) {
    const dayDate = new Date(today.getFullYear(), today.getMonth(), today.getDate() - i, 23, 59, 59, 999);
    const dayKey = dayKeyFromDateValue_(dayDate.toISOString());
    const cutoff14d = new Date(dayDate.getTime() - 14 * 24 * 60 * 60 * 1000);
    const rawRowsToDay = filterRowsUpToDate_(rawRows, dayDate);
    const workoutRowsToDay = filterRowsUpToDate_(workoutRows, dayDate);
    const energyDay = computeEnergyReadiness_(rawRowsToDay, workoutRowsToDay, dayDate, cutoff14d);

    if (!grouped[dayKey]) grouped[dayKey] = {};
    if (grouped[dayKey].sleep_quality === '' || grouped[dayKey].sleep_quality === null || grouped[dayKey].sleep_quality === undefined) {
      grouped[dayKey].sleep_quality = energyDay.sleepScore;
    }
    if (grouped[dayKey].readiness_score === '' || grouped[dayKey].readiness_score === null || grouped[dayKey].readiness_score === undefined) {
      grouped[dayKey].readiness_score = energyDay.score;
    }
  }

  Object.keys(grouped).forEach(key => {
    const day = grouped[key];
    if (day.hrv_values && day.hrv_values.length) {
      day.hrv = average_(day.hrv_values);
    }
  });

  const output = [READINESS_HEADERS];
  for (let i = 0; i < 50; i++) {
    const d = new Date(today.getFullYear(), today.getMonth(), today.getDate() - i);
    const key = dayKeyFromDateValue_(d.toISOString());
    const auto = grouped[key] || {};
    const manual = existingManual[key] || {};

    output.push([
      formatDateDdMmYyyy_(d),
      valueOrBlank_(auto.sleep_hours),
      valueOrBlank_(auto.sleep_quality),
      valueOrBlank_(auto.readiness_score),
      valueOrBlank_(auto.hr_resting),
      valueOrBlank_(auto.hrv),
      valueOrBlank_(manual.fatiga_percibida),
      valueOrBlank_(manual.energia_autoinformada),
      valueOrBlank_(auto.peso_kg),
      valueOrBlank_(auto.fuente),
      formatDateForDisplay_(auto.ultima_actualizacion || new Date().toISOString()),
      valueOrBlank_(manual.notas)
    ]);
  }

  sheet.getRange(1, 1, output.length, READINESS_HEADERS.length).setValues(output);
  formatReadinessSheet_(sheet, output.length - 1);
}

function getExistingReadinessManualMap_(sheet) {
  const map = {};
  if (!sheet || sheet.getLastRow() < 2) return map;
  const data = sheet.getDataRange().getValues();
  const header = data[0];
  const idxFecha = header.indexOf('Fecha (DD-MM-YYYY)');
  const idxFatiga = header.indexOf('Fatiga_percibida');
  const idxEnergia = header.indexOf('Energia_autoinformada');
  const idxNotas = header.indexOf('Notas');
  if (idxFecha === -1) return map;

  data.slice(1).forEach(r => {
    const key = normalizeDateKeyFromDisplay_(r[idxFecha]);
    if (!key) return;
    map[key] = {
      fatiga_percibida: idxFatiga === -1 ? '' : r[idxFatiga],
      energia_autoinformada: idxEnergia === -1 ? '' : r[idxEnergia],
      notas: idxNotas === -1 ? '' : r[idxNotas]
    };
  });
  return map;
}

function formatReadinessSheet_(sheet, rowCount) {
  try {
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, READINESS_HEADERS.length)
      .setFontWeight('bold')
      .setBackground('#d9ead3')
      .setHorizontalAlignment('center');
    if (rowCount <= 0) return;

    sheet.getRange(2, 1, rowCount, 1).setHorizontalAlignment('center');
    sheet.getRange(2, 11, rowCount, 1).setHorizontalAlignment('center');

    [2, 3, 4, 5, 6, 7, 8, 9].forEach(col => {
      sheet.getRange(2, col, rowCount, 1).setNumberFormat('0.00');
    });
    [3, 4, 7, 8].forEach(col => {
      sheet.getRange(2, col, rowCount, 1).setNumberFormat('0');
    });

    sheet.getRange(2, 1, rowCount, READINESS_HEADERS.length)
      .setBorder(true, true, true, true, true, true);
    autoResizeSomeColumns_(sheet, READINESS_HEADERS.length);
  } catch (e) {}
}

function getRowsAsObjects_(sheet) {
  if (!sheet || sheet.getLastRow() < 2) return [];
  const data = sheet.getDataRange().getValues();
  const header = data[0];
  return data.slice(1).map(r => rowArrayToObject_(header, r));
}

function assignIfNotBlank_(obj, key, value) {
  if (value === '' || value === null || value === undefined) return;
  obj[key] = value;
}

function dayKeyFromDateValue_(value) {
  const t = toTimeMs_(value);
  if (t === null) return '';
  const d = new Date(t);
  return d.getFullYear() + '-' + pad2_(d.getMonth() + 1) + '-' + pad2_(d.getDate());
}

function normalizeDateKeyFromDisplay_(value) {
  const s = String(value || '').trim();
  const m = s.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (!m) return '';
  return m[3] + '-' + m[2] + '-' + m[1];
}

function formatDateDdMmYyyy_(value) {
  const d = new Date(value);
  if (isNaN(d.getTime())) return '';
  return pad2_(d.getDate()) + '-' + pad2_(d.getMonth() + 1) + '-' + d.getFullYear();
}

function valueOrBlank_(value) {
  return value === null || value === undefined ? '' : value;
}

function filterRowsUpToDate_(rows, maxDate) {
  if (!rows || !rows.length) return [];
  const maxTime = maxDate.getTime();
  return rows.filter(row => {
    const t = toTimeMs_(row.date_iso || row.date || row.received_at || row.start_iso || row.start || row.startDate);
    return t !== null && t <= maxTime;
  });
}

function extractHrvValue_(row) {
  const candidates = [
    row.qty_num,
    row.qty,
    row.Avg_num,
    row.Avg,
    row.sdnn,
    row.hrv,
    row.value_num,
    row.value
  ];

  for (let i = 0; i < candidates.length; i++) {
    const parsed = toNumberOrBlank_(candidates[i]);
    if (parsed !== '') return parsed;
  }
  return '';
}

function rowArrayToObject_(header, row) {
  const obj = {};
  for (let i = 0; i < header.length; i++) obj[header[i]] = row[i];
  return obj;
}

function formatRowForDisplay_(header, row) {
  return row.map((value, idx) => formatFieldForDisplay_(header[idx], value));
}

function formatDataSheet_(sheet, header, rowCount) {
  try {
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, header.length)
      .setFontWeight('bold')
      .setBackground('#d9ead3')
      .setHorizontalAlignment('center');

    if (rowCount > 0) {
      const dateFields = ['received_at', 'date', 'date_iso', 'sleepStart', 'sleepEnd', 'inBedStart', 'inBedEnd'];
      const decimal2Fields = ['totalSleep', 'totalSleep_num', 'deep', 'deep_num', 'rem', 'rem_num', 'core', 'core_num', 'awake', 'awake_num', 'inBed', 'inBed_num', 'Min', 'Min_num', 'Max', 'Max_num', 'Avg', 'Avg_num'];

      dateFields.forEach(field => {
        const idx = header.indexOf(field);
        if (idx !== -1) {
          sheet.getRange(2, idx + 1, rowCount, 1).setHorizontalAlignment('center');
        }
      });

      decimal2Fields.forEach(field => {
        const idx = header.indexOf(field);
        if (idx !== -1) {
          sheet.getRange(2, idx + 1, rowCount, 1).setNumberFormat('0.00');
        }
      });

      const qtyIdx = header.indexOf('qty_num');
      if (qtyIdx !== -1) {
        sheet.getRange(2, qtyIdx + 1, rowCount, 1).setNumberFormat('0.##');
      }

      sheet.getRange(2, 1, rowCount, header.length).setBorder(true, true, true, true, true, true);
    }
  } catch (e) {}
}

function getLatestQty_(rows, metricName) {
  const item = getLatestRow_(rows, metricName);
  if (!item) return '';
  return toNumberOrBlank_(item.qty_num !== '' ? item.qty_num : item.qty);
}

function getLatestField_(rows, metricName, fieldName) {
  const item = getLatestRow_(rows, metricName);
  if (!item) return '';
  return toNumberOrBlank_(item[fieldName]);
}

function getLatestRow_(rows, metricName) {
  const filtered = rows
    .filter(r => normalizeMetricName_(r.metric_name) === metricName)
    .filter(r => r.date_iso)
    .sort((a, b) => (toTimeMs_(b.date_iso) || -Infinity) - (toTimeMs_(a.date_iso) || -Infinity));

  return filtered.length ? filtered[0] : null;
}

function getAverageQtySince_(rows, metricName, sinceDate) {
  const vals = rows
    .filter(r => normalizeMetricName_(r.metric_name) === metricName)
    .filter(r => isOnOrAfter_(r.date_iso, sinceDate))
    .map(r => toNumberOrBlank_(r.qty_num !== '' ? r.qty_num : r.qty))
    .filter(v => v !== '');

  return average_(vals);
}

function getAverageFieldSince_(rows, metricName, fieldName, sinceDate) {
  const vals = rows
    .filter(r => normalizeMetricName_(r.metric_name) === metricName)
    .filter(r => isOnOrAfter_(r.date_iso, sinceDate))
    .map(r => toNumberOrBlank_(r[fieldName]))
    .filter(v => v !== '');

  return average_(vals);
}

function getLatestReceivedAt_(rows) {
  const vals = rows
    .map(r => r.received_at)
    .filter(Boolean)
    .sort((a, b) => (toTimeMs_(b) || -Infinity) - (toTimeMs_(a) || -Infinity));

  return vals.length ? vals[0] : '';
}

function average_(arr) {
  if (!arr.length) return '';
  const sum = arr.reduce((a, b) => a + b, 0);
  return sum / arr.length;
}

function isOnOrAfter_(dateValue, sinceDate) {
  const t = toTimeMs_(dateValue);
  return t !== null && t >= sinceDate.getTime();
}

function toTimeMs_(dateValue) {
  if (!dateValue) return null;
  const d = new Date(dateValue);
  return isNaN(d.getTime()) ? null : d.getTime();
}

function formatMetricValue_(value, decimals) {
  if (value === '' || value === null || value === undefined) return '';
  if (typeof value !== 'number') {
    const n = Number(value);
    if (isNaN(n)) return String(value);
    value = n;
  }
  return value.toFixed(decimals);
}

function flattenObjectRecursive_(value, target, prefix) {
  const key = prefix || 'value';

  if (value === null || value === undefined) {
    target[key] = '';
    return;
  }

  if (Array.isArray(value)) {
    target[key] = truncateForCell_(safeJsonStringify_(value), MAX_CELL_CHARS);
    return;
  }

  if (typeof value === 'object') {
    const keys = Object.keys(value);

    if (!keys.length) {
      target[key] = '{}';
      return;
    }

    keys.forEach(childKey => {
      const newPrefix = prefix ? `${prefix}.${childKey}` : childKey;
      flattenObjectRecursive_(value[childKey], target, newPrefix);
    });
    return;
  }

  target[key] = sanitizeCellValue_(value);
}

function ensureHeader_(sheet, keys) {
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, keys.length).setValues([keys]);
    return;
  }

  const existing = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const missing = keys.filter(k => !existing.includes(k));

  if (missing.length) {
    sheet.getRange(1, existing.length + 1, 1, missing.length).setValues([missing]);
  }
}

function collectAllKeys_(rows) {
  const set = {};
  rows.forEach(row => {
    Object.keys(row).forEach(k => {
      set[k] = true;
    });
  });
  return Object.keys(set);
}

function getOrCreateSheet_(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  return sheet;
}

function normalizeMetricName_(name) {
  return String(name || '').trim();
}

function normalizeDateString_(value) {
  if (!value) return '';
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value.getTime())) {
    return value.toISOString();
  }

  const s = String(value).trim();
  if (!s) return '';

  const normalized = s
    .replace(/^(\d{4}-\d{2}-\d{2}) /, '$1T')
    .replace(/ ([+-]\d{2}:?\d{2})$/, '$1');
  const d = new Date(normalized);

  if (!isNaN(d.getTime())) return d.toISOString();

  const fallback = new Date(s);
  if (!isNaN(fallback.getTime())) return fallback.toISOString();

  return s;
}

function toNumberOrBlank_(value) {
  if (value === '' || value === null || value === undefined) return '';
  if (typeof value === 'number') return value;

  const s = String(value).replace(',', '.').trim();
  const n = Number(s);
  return isNaN(n) ? '' : n;
}

function normalizeNumberForKey_(value) {
  const n = toNumberOrBlank_(value);
  return n === '' ? '' : String(n);
}

function looksLikeJson_(text) {
  const t = String(text || '').trim();
  return t.startsWith('{') || t.startsWith('[');
}

function sanitizeCellValue_(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return truncateForCell_(value, MAX_CELL_CHARS);
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  return truncateForCell_(safeJsonStringify_(value), MAX_CELL_CHARS);
}

function truncateForCell_(text, maxLen) {
  const s = String(text || '');
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen - 30) + '...[TRUNCADO ' + s.length + ' chars]';
}

function safeJsonStringify_(obj) {
  try {
    return JSON.stringify(obj);
  } catch (err) {
    return '[JSON stringify error: ' + String(err) + ']';
  }
}



function jsonResponse_(obj, status) {
  return ContentService
    .createTextOutput(JSON.stringify({ app_status: status, ...obj }))
    .setMimeType(ContentService.MimeType.JSON);
}

function logDebug_(stage, data) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = getOrCreateSheet_(ss, DEBUG_SHEET_NAME);

    if (sheet.getLastRow() === 0) {
      sheet.getRange(1, 1, 1, 3).setValues([['timestamp', 'stage', 'data']]);
    }

    sheet.appendRow([
      new Date().toISOString(),
      truncateForCell_(stage, 200),
      truncateForCell_(safeJsonStringify_(data), 20000)
    ]);

    const maxRows = 500;
    const lastRow = sheet.getLastRow();
    if (lastRow > maxRows + 1) {
      sheet.deleteRows(2, lastRow - (maxRows + 1));
    }
  } catch (e) {
    console.log(stage + ' ' + safeJsonStringify_(data));
  }
}

function autoResizeSomeColumns_(sheet, count) {
  for (let i = 1; i <= count; i++) {
    try {
      sheet.autoResizeColumn(i);
    } catch (e) {}
  }
}
