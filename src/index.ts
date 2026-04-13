import express, { Request, Response } from "express";
import axios from "axios";
import cron from "node-cron";
import dotenv from "dotenv";
import path from "path";

dotenv.config({
  path: path.resolve(process.cwd(), ".env"),
  override: true,
});

type WixDataItem = Record<string, unknown>;

interface WixApiResponse {
  pagingMetadata?: { hasNext?: boolean };
  dataItems?: WixDataItem[];
  items?: WixDataItem[];
  currentPage?: WixDataItem[];
}

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const WIX_QUERY_URL = "https://www.wixapis.com/wix-data/v2/items/query";
const WIX_DELETE_BASE_URL = "https://www.wixapis.com/wix-data/v2/items";
const COLLECTION_ID = process.env.WIX_COLLECTION_ID || "Import1";
const PAGE_LIMIT = Number(process.env.WIX_PAGE_LIMIT || 100);
const DEDUPE_FIELDS = ["project", "gender", "roleName"];
const SHOOT_DATE_FIELD = process.env.WIX_SHOOT_DATE_FIELD?.trim() || "shoot_date";
const STALE_NA_MONTHS = Math.max(
  1,
  Number(process.env.STALE_NA_MONTHS || 2) || 2
);
/** If set (e.g. Asia/Karachi), "today" vs shoot_date uses this timezone; else server local. */
const CLEANUP_DATE_TIMEZONE = process.env.CLEANUP_DATE_TIMEZONE?.trim() || undefined;

function getCronExpression(): string | null {
  const raw = process.env.CRON_EXPRESSION;
  const expr =
    raw === undefined || raw === null || raw.trim() === ""
      ? "0 3 * * *"
      : raw.trim();
  if (/^(off|false|disable|disabled)$/i.test(expr)) {
    return null;
  }
  return expr;
}

/** Second job: delete by shoot_date rules. Default 03:30 daily (after dedupe). */
function getCleanupCronExpression(): string | null {
  const raw = process.env.CRON_CLEANUP_EXPRESSION;
  const expr =
    raw === undefined || raw === null || raw.trim() === ""
      ? "30 3 * * *"
      : raw.trim();
  if (/^(off|false|disable|disabled)$/i.test(expr)) {
    return null;
  }
  return expr;
}

function validateEnv(): string[] {
  const required = ["WIX_AUTH_TOKEN", "WIX_SITE_ID"];
  return required.filter((key) => !process.env[key]);
}

function getValue(item: WixDataItem, key: string): unknown {
  const rootValue = item[key];
  const dataValue =
    item.data && typeof item.data === "object"
      ? (item.data as Record<string, unknown>)[key]
      : undefined;
  return rootValue ?? dataValue;
}

function normalizeValue(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized || null;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value).toLowerCase();
  }
  return null;
}

function getCompositeKey(item: WixDataItem): string | null {
  const parts: string[] = [];
  for (const field of DEDUPE_FIELDS) {
    const normalized = normalizeValue(getValue(item, field));
    if (!normalized) {
      return null;
    }
    parts.push(normalized);
  }
  return parts.join("||");
}

function getItemId(item: WixDataItem): string | null {
  const maybeId = item.id || item._id || item.dataItemId;
  return typeof maybeId === "string" && maybeId.trim() ? maybeId : null;
}

async function fetchPage(offset: number): Promise<WixApiResponse> {
  const payload = {
    dataCollectionId: COLLECTION_ID,
    query: {
      sort: [{ fieldName: "_createdDate", order: "desc" }],
      paging: { limit: PAGE_LIMIT, offset },
    },
  };

  const response = await axios.post<WixApiResponse>(WIX_QUERY_URL, payload, {
    headers: {
      Authorization: process.env.WIX_AUTH_TOKEN,
      "Content-Type": "application/json",
      Accept: "application/json",
      "wix-site-id": process.env.WIX_SITE_ID,
    },
    timeout: 30000,
  });

  return response.data || {};
}

function extractItems(page: WixApiResponse): WixDataItem[] {
  const raw =
    page.dataItems || page.items || page.currentPage || ([] as WixDataItem[]);

  return raw.map((entry) => {
    if (entry && typeof entry === "object" && entry.dataItem) {
      const nested = entry.dataItem;
      return typeof nested === "object" && nested ? (nested as WixDataItem) : entry;
    }
    return entry;
  });
}

async function deleteInBatches(
  ids: string[],
  batchSize: number,
  onBatchDone: (deleted: number, failed: number) => void
): Promise<{
  deleted: number;
  failed: number;
  failureReasons: Record<string, number>;
}> {
  let deleted = 0;
  let failed = 0;
  const failureReasons: Record<string, number> = {};

  const getReasonKey = (error: unknown): string => {
    if (axios.isAxiosError(error)) {
      const status = error.response?.status;
      if (status) {
        return `HTTP_${status}`;
      }
      const code = error.code || "AXIOS_UNKNOWN";
      return `NET_${code}`;
    }
    return "UNKNOWN";
  };

  const deleteWithRetry = async (id: string): Promise<void> => {
    const maxRetries = 2;
    let attempt = 0;
    while (true) {
      try {
        await deleteItem(id);
        return;
      } catch (error) {
        const reason = getReasonKey(error);
        const isRetryable =
          reason.startsWith("NET_") || reason === "HTTP_429" || reason === "HTTP_503";
        if (attempt >= maxRetries || !isRetryable) {
          throw error;
        }
        attempt += 1;
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
      }
    }
  };

  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    for (const id of chunk) {
      try {
        await deleteWithRetry(id);
        deleted += 1;
      } catch (error) {
        failed += 1;
        const reason = getReasonKey(error);
        failureReasons[reason] = (failureReasons[reason] || 0) + 1;
      }
    }
    onBatchDone(deleted, failed);
  }

  return { deleted, failed, failureReasons };
}

async function deleteItem(itemId: string): Promise<void> {
  await axios.delete(`${WIX_DELETE_BASE_URL}/${encodeURIComponent(itemId)}`, {
    params: { dataCollectionId: COLLECTION_ID },
    headers: {
      Authorization: process.env.WIX_AUTH_TOKEN,
      Accept: "application/json",
      "wix-site-id": process.env.WIX_SITE_ID,
    },
    timeout: 30000,
  });
}

function streamEvent(
  res: Response,
  event: string,
  payload: Record<string, unknown>
): void {
  if (res.writableEnded || res.destroyed) {
    return;
  }
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function logInfo(message: string, meta?: Record<string, unknown>): void {
  if (meta) {
    console.log(`[DEDUPE] ${message}`, meta);
    return;
  }
  console.log(`[DEDUPE] ${message}`);
}

function logError(message: string, meta?: Record<string, unknown>): void {
  if (meta) {
    console.error(`[DEDUPE] ${message}`, meta);
    return;
  }
  console.error(`[DEDUPE] ${message}`);
}

function logCleanupInfo(message: string, meta?: Record<string, unknown>): void {
  if (meta) {
    console.log(`[CLEANUP] ${message}`, meta);
    return;
  }
  console.log(`[CLEANUP] ${message}`);
}

function logCleanupError(message: string, meta?: Record<string, unknown>): void {
  if (meta) {
    console.error(`[CLEANUP] ${message}`, meta);
    return;
  }
  console.error(`[CLEANUP] ${message}`);
}

function startOfLocalDay(d = new Date()): Date {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function addCalendarMonths(base: Date, deltaMonths: number): Date {
  return new Date(
    base.getFullYear(),
    base.getMonth() + deltaMonths,
    base.getDate(),
    base.getHours(),
    base.getMinutes(),
    base.getSeconds(),
    base.getMilliseconds()
  );
}

function parseWixDate(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof value === "object" && value !== null && "$date" in value) {
    return parseWixDate((value as { $date: unknown }).$date);
  }
  if (typeof value === "string") {
    const d = new Date(value.trim());
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

function getItemCreatedAt(item: WixDataItem): Date | null {
  return (
    parseWixDate(getValue(item, "_createdDate")) ||
    parseWixDate(getValue(item, "createdAt")) ||
    parseWixDate(getValue(item, "_dateCreated"))
  );
}

function isShootDateNA(value: unknown): boolean {
  if (value === null || value === undefined) {
    return true;
  }
  if (typeof value === "string" && !value.trim()) {
    return true;
  }
  const s = normalizeValue(value);
  if (s === null) {
    return true;
  }
  return (
    s === "n/a" ||
    s === "na" ||
    s === "n.a." ||
    s === "-" ||
    s === "none" ||
    s === "tbd" ||
    s === "pending"
  );
}

/** Returns a calendar date if parseable and not N/A; null if N/A/empty; invalid string → null (no delete). */
function parseShootCalendarDate(value: unknown): Date | null {
  if (isShootDateNA(value)) {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof value === "object" && value !== null && "$date" in value) {
    return parseShootCalendarDate((value as { $date: unknown }).$date);
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const isoTry = new Date(trimmed);
  if (!Number.isNaN(isoTry.getTime())) {
    return isoTry;
  }
  const m = trimmed.match(/^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2,4})$/);
  if (m) {
    const a = Number.parseInt(m[1], 10);
    const b = Number.parseInt(m[2], 10);
    let y = Number.parseInt(m[3], 10);
    if (m[3].length === 2) {
      y += y >= 70 ? 1900 : 2000;
    }
    const dmy = new Date(y, b - 1, a);
    if (
      !Number.isNaN(dmy.getTime()) &&
      dmy.getFullYear() === y &&
      dmy.getMonth() === b - 1 &&
      dmy.getDate() === a
    ) {
      return dmy;
    }
    const mdy = new Date(y, a - 1, b);
    if (
      !Number.isNaN(mdy.getTime()) &&
      mdy.getFullYear() === y &&
      mdy.getMonth() === a - 1 &&
      mdy.getDate() === b
    ) {
      return mdy;
    }
  }
  return null;
}

function isShootDayBeforeToday(
  shoot: Date,
  now: Date,
  timeZone?: string
): boolean {
  if (timeZone) {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const todayStr = fmt.format(now);
    const shootStr = fmt.format(shoot);
    return shootStr < todayStr;
  }
  const todayStart = startOfLocalDay(now);
  const day = new Date(
    shoot.getFullYear(),
    shoot.getMonth(),
    shoot.getDate()
  );
  return day < todayStart;
}

function isCreatedOlderThanMonths(created: Date, months: number, now: Date): boolean {
  const cutoff = addCalendarMonths(now, -months);
  return created < cutoff;
}

type StaleDeleteReason = "past_shoot_date" | "na_stale_by_created";

function collectStaleShootItemIds(
  items: WixDataItem[],
  now: Date,
  naMonths: number,
  dateTimeZone?: string
): { ids: string[]; reasons: Record<StaleDeleteReason, number> } {
  const ids: string[] = [];
  const reasons: Record<StaleDeleteReason, number> = {
    past_shoot_date: 0,
    na_stale_by_created: 0,
  };

  for (const item of items) {
    const itemId = getItemId(item);
    if (!itemId) {
      continue;
    }

    const rawShoot = getValue(item, SHOOT_DATE_FIELD);
    const parsedShoot = parseShootCalendarDate(rawShoot);

    if (parsedShoot !== null) {
      if (isShootDayBeforeToday(parsedShoot, now, dateTimeZone)) {
        ids.push(itemId);
        reasons.past_shoot_date += 1;
      }
      continue;
    }

    if (!isShootDateNA(rawShoot)) {
      continue;
    }

    const created = getItemCreatedAt(item);
    if (!created) {
      continue;
    }
    if (isCreatedOlderThanMonths(created, naMonths, now)) {
      ids.push(itemId);
      reasons.na_stale_by_created += 1;
    }
  }

  return { ids, reasons };
}

type RunDedupeJobOptions = {
  deleteBatchSize: number;
  runId: string;
  /** e.g. "GET" | "POST" | "cron" for logs */
  source: string;
  onEvent?: (event: string, payload: Record<string, unknown>) => void;
  getClientDisconnected?: () => boolean;
};

async function runDedupeJob(options: RunDedupeJobOptions): Promise<void> {
  const {
    deleteBatchSize,
    runId,
    source,
    onEvent,
    getClientDisconnected,
  } = options;
  const emit = (event: string, payload: Record<string, unknown>) => {
    onEvent?.(event, payload);
  };

  const seen = new Set<string>();
  let offset = 0;
  let hasNext = true;
  let pages = 0;
  let processed = 0;
  let duplicates = 0;
  let deleted = 0;
  let failedDeletes = 0;
  let deleteFailureReasons: Record<string, number> = {};
  const duplicateIdsToDelete: string[] = [];

  logInfo("Run started", {
    runId,
    source,
    collectionId: COLLECTION_ID,
    pageLimit: PAGE_LIMIT,
    dedupeFields: DEDUPE_FIELDS,
  });

  emit("start", {
    ok: true,
    endpoint: "/dedupe-wix-items",
    runId,
    collectionId: COLLECTION_ID,
    dedupeFields: DEDUPE_FIELDS,
  });

  while (hasNext) {
    const page = await fetchPage(offset);
    const items = extractItems(page);
    hasNext = Boolean(page.pagingMetadata?.hasNext);
    pages += 1;

    logInfo("Fetched page", {
      runId,
      page: pages,
      fetchedItems: items.length,
      hasNext,
      offset,
    });

    for (const item of items) {
      processed += 1;
      const key = getCompositeKey(item);
      if (!key) {
        continue;
      }

      if (!seen.has(key)) {
        seen.add(key);
        continue;
      }

      duplicates += 1;

      const itemId = getItemId(item);
      if (!itemId) {
        failedDeletes += 1;
        continue;
      }

      duplicateIdsToDelete.push(itemId);
    }

    emit("progress", {
      runId,
      page: pages,
      processed,
      unique: seen.size,
      duplicates,
      deleted,
      failedDeletes,
    });
    logInfo("Progress", {
      runId,
      page: pages,
      processed,
      unique: seen.size,
      duplicates,
      deleted,
      failedDeletes,
    });

    offset += PAGE_LIMIT;
  }

  if (duplicateIdsToDelete.length > 0) {
    emit("delete_start", {
      runId,
      totalDuplicateIds: duplicateIdsToDelete.length,
      batchSize: deleteBatchSize,
    });

    const deleteResult = await deleteInBatches(
      duplicateIdsToDelete,
      Math.max(1, deleteBatchSize),
      (deletedCount, failedCount) => {
        deleted = deletedCount;
        failedDeletes = failedCount;
        emit("delete_progress", {
          runId,
          deleted,
          failedDeletes,
          totalDuplicateIds: duplicateIdsToDelete.length,
        });
        logInfo("Delete batch progress", {
          runId,
          deleted,
          failedDeletes,
          totalDuplicateIds: duplicateIdsToDelete.length,
        });
      }
    );

    deleted = deleteResult.deleted;
    failedDeletes = deleteResult.failed;
    deleteFailureReasons = deleteResult.failureReasons;
    logInfo("Delete phase summary", {
      runId,
      deleted,
      failedDeletes,
      failureReasons: deleteFailureReasons,
    });
  }

  const clientDisconnected = getClientDisconnected?.() ?? false;

  emit("done", {
    ok: true,
    runId,
    clientDisconnected,
    collectionId: COLLECTION_ID,
    dedupeFields: DEDUPE_FIELDS,
    stats: {
      pages,
      processed,
      unique: seen.size,
      duplicates,
      deleted,
      failedDeletes,
      queuedForDelete: duplicateIdsToDelete.length,
      failureReasons: deleteFailureReasons,
    },
  });
  logInfo("Run completed", {
    runId,
    clientDisconnected,
    pages,
    processed,
    unique: seen.size,
    duplicates,
    deleted,
    failedDeletes,
  });
}

type RunStaleShootCleanupOptions = {
  deleteBatchSize: number;
  runId: string;
  source: string;
  onEvent?: (event: string, payload: Record<string, unknown>) => void;
  getClientDisconnected?: () => boolean;
};

async function runStaleShootCleanupJob(
  options: RunStaleShootCleanupOptions
): Promise<void> {
  const {
    deleteBatchSize,
    runId,
    source,
    onEvent,
    getClientDisconnected,
  } = options;
  const emit = (event: string, payload: Record<string, unknown>) => {
    onEvent?.(event, payload);
  };

  const now = new Date();
  let offset = 0;
  let hasNext = true;
  let pages = 0;
  let processed = 0;
  const idSet = new Set<string>();
  const reasonTotals: Record<StaleDeleteReason, number> = {
    past_shoot_date: 0,
    na_stale_by_created: 0,
  };
  let deleted = 0;
  let failedDeletes = 0;
  let deleteFailureReasons: Record<string, number> = {};

  logCleanupInfo("Run started", {
    runId,
    source,
    collectionId: COLLECTION_ID,
    shootDateField: SHOOT_DATE_FIELD,
    staleNaMonths: STALE_NA_MONTHS,
    dateTimezone: CLEANUP_DATE_TIMEZONE ?? "server local",
  });

  emit("start", {
    ok: true,
    endpoint: "/cleanup-by-shoot-date",
    runId,
    collectionId: COLLECTION_ID,
    shootDateField: SHOOT_DATE_FIELD,
    staleNaMonths: STALE_NA_MONTHS,
    dateTimezone: CLEANUP_DATE_TIMEZONE ?? null,
  });

  while (hasNext) {
    const page = await fetchPage(offset);
    const items = extractItems(page);
    hasNext = Boolean(page.pagingMetadata?.hasNext);
    pages += 1;

    const { ids, reasons } = collectStaleShootItemIds(
      items,
      now,
      STALE_NA_MONTHS,
      CLEANUP_DATE_TIMEZONE
    );
    for (const id of ids) {
      idSet.add(id);
    }
    reasonTotals.past_shoot_date += reasons.past_shoot_date;
    reasonTotals.na_stale_by_created += reasons.na_stale_by_created;

    processed += items.length;

    emit("progress", {
      runId,
      page: pages,
      processed,
      queuedUniqueIds: idSet.size,
      reasonsThisPage: reasons,
    });
    logCleanupInfo("Progress", {
      runId,
      page: pages,
      processed,
      queuedUniqueIds: idSet.size,
      reasons: { ...reasonTotals },
    });

    offset += PAGE_LIMIT;
  }

  const idsToDelete = [...idSet];

  if (idsToDelete.length > 0) {
    emit("delete_start", {
      runId,
      totalIds: idsToDelete.length,
      batchSize: deleteBatchSize,
    });

    const deleteResult = await deleteInBatches(
      idsToDelete,
      Math.max(1, deleteBatchSize),
      (deletedCount, failedCount) => {
        deleted = deletedCount;
        failedDeletes = failedCount;
        emit("delete_progress", {
          runId,
          deleted,
          failedDeletes,
          totalIds: idsToDelete.length,
        });
        logCleanupInfo("Delete batch progress", {
          runId,
          deleted,
          failedDeletes,
          totalIds: idsToDelete.length,
        });
      }
    );

    deleted = deleteResult.deleted;
    failedDeletes = deleteResult.failed;
    deleteFailureReasons = deleteResult.failureReasons;
  }

  const clientDisconnected = getClientDisconnected?.() ?? false;

  emit("done", {
    ok: true,
    runId,
    clientDisconnected,
    collectionId: COLLECTION_ID,
    stats: {
      pages,
      processed,
      queuedForDelete: idsToDelete.length,
      deleteReasons: reasonTotals,
      deleted,
      failedDeletes,
      failureReasons: deleteFailureReasons,
    },
  });

  logCleanupInfo("Run completed", {
    runId,
    clientDisconnected,
    pages,
    processed,
    queuedForDelete: idsToDelete.length,
    reasons: reasonTotals,
    deleted,
    failedDeletes,
  });
}

const cleanupShootHandler = async (req: Request, res: Response) => {
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      message: "Use GET or POST on /cleanup-by-shoot-date",
    });
  }

  const missing = validateEnv();
  if (missing.length > 0) {
    return res.status(400).json({
      ok: false,
      message: "Missing required environment variables",
      missingEnv: missing,
    });
  }

  const deleteBatchSize =
    req.method === "GET"
      ? Number(req.query.batchSize || 100)
      : Number(
          (req.body as Record<string, unknown> | undefined)?.batchSize || 100
        );
  const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  let clientDisconnected = false;
  let completed = false;
  req.on("close", () => {
    if (!completed) {
      clientDisconnected = true;
      logCleanupInfo("Client disconnected", { runId });
    }
  });

  try {
    await runStaleShootCleanupJob({
      deleteBatchSize,
      runId,
      source: req.method,
      onEvent: (event, payload) => streamEvent(res, event, payload),
      getClientDisconnected: () => clientDisconnected,
    });

    completed = true;
    if (!res.writableEnded && !res.destroyed) {
      return res.end();
    }
    return;
  } catch (error: unknown) {
    if (axios.isAxiosError(error)) {
      logCleanupError("Wix API request failed", {
        runId,
        status: error.response?.status || 500,
        details: error.response?.data || error.message,
      });
      streamEvent(res, "error", {
        ok: false,
        runId,
        message: "Wix API request failed",
        details: error.response?.data || error.message,
      });
      completed = true;
      if (!res.writableEnded && !res.destroyed) {
        return res.end();
      }
      return;
    }

    logCleanupError("Unexpected error", {
      runId,
      details: error instanceof Error ? error.message : "Unknown error",
    });
    streamEvent(res, "error", {
      ok: false,
      runId,
      message: "Unexpected error",
      details: error instanceof Error ? error.message : "Unknown error",
    });
    completed = true;
    if (!res.writableEnded && !res.destroyed) {
      return res.end();
    }
    return;
  }
};

const dedupeHandler = async (req: Request, res: Response) => {
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      message: "Use GET or POST on /dedupe-wix-items",
    });
  }

  const missing = validateEnv();
  if (missing.length > 0) {
    return res.status(400).json({
      ok: false,
      message: "Missing required environment variables",
      missingEnv: missing,
    });
  }

  const deleteBatchSize =
    req.method === "GET"
      ? Number(req.query.batchSize || 100)
      : Number(req.body?.batchSize || 100);
  const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  let clientDisconnected = false;
  let completed = false;
  req.on("close", () => {
    if (!completed) {
      clientDisconnected = true;
      logInfo("Client disconnected", { runId });
    }
  });

  try {
    await runDedupeJob({
      deleteBatchSize,
      runId,
      source: req.method,
      onEvent: (event, payload) => streamEvent(res, event, payload),
      getClientDisconnected: () => clientDisconnected,
    });

    completed = true;
    if (!res.writableEnded && !res.destroyed) {
      return res.end();
    }
    return;
  } catch (error: unknown) {
    if (axios.isAxiosError(error)) {
      logError("Wix API request failed", {
        runId,
        status: error.response?.status || 500,
        details: error.response?.data || error.message,
      });
      streamEvent(res, "error", {
        ok: false,
        runId,
        message: "Wix API request failed",
        details: error.response?.data || error.message,
      });
      completed = true;
      if (!res.writableEnded && !res.destroyed) {
        return res.end();
      }
      return;
    }

    logError("Unexpected error", {
      runId,
      details: error instanceof Error ? error.message : "Unknown error",
    });
    streamEvent(res, "error", {
      ok: false,
      runId,
      message: "Unexpected error",
      details: error instanceof Error ? error.message : "Unknown error",
    });
    completed = true;
    if (!res.writableEnded && !res.destroyed) {
      return res.end();
    }
    return;
  }
};

app.all("/dedupe-wix-items", dedupeHandler);
app.all("/cleanup-by-shoot-date", cleanupShootHandler);

app.use((err: unknown, _req: Request, res: Response, _next: unknown) => {
  if (err instanceof SyntaxError) {
    return res.status(400).json({
      ok: false,
      message: "Invalid JSON body. Send valid JSON or use GET.",
    });
  }
  return res.status(500).json({
    ok: false,
    message: "Unexpected server error",
  });
});

const server = app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);

  const cronTz = process.env.CRON_TIMEZONE?.trim() || undefined;
  const cronOptions = cronTz ? { timezone: cronTz } : undefined;

  const dedupeExpr = getCronExpression();
  if (dedupeExpr) {
    const cronBatchSize = Math.max(1, Number(process.env.CRON_BATCH_SIZE || 100));
    cron.schedule(
      dedupeExpr,
      () => {
        void (async () => {
          const missing = validateEnv();
          if (missing.length > 0) {
            logError("Scheduled dedupe skipped: missing env", { missing });
            return;
          }
          const runId = `cron-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
          try {
            await runDedupeJob({
              deleteBatchSize: cronBatchSize,
              runId,
              source: "cron",
            });
          } catch (error: unknown) {
            if (axios.isAxiosError(error)) {
              logError("Cron dedupe: Wix API failed", {
                runId,
                status: error.response?.status,
                details: error.response?.data || error.message,
              });
              return;
            }
            logError("Cron dedupe failed", {
              runId,
              details: error instanceof Error ? error.message : "Unknown error",
            });
          }
        })();
      },
      cronOptions
    );
    logInfo("Scheduled dedupe cron active", {
      expression: dedupeExpr,
      timezone: cronTz ?? "server local time",
      batchSize: cronBatchSize,
    });
  } else {
    logInfo(
      "Dedupe cron disabled (CRON_EXPRESSION=off). Empty CRON_EXPRESSION = default 03:00 daily."
    );
  }

  const cleanupExpr = getCleanupCronExpression();
  if (cleanupExpr) {
    const cleanupBatchSize = Math.max(
      1,
      Number(
        process.env.CRON_CLEANUP_BATCH_SIZE ||
          process.env.CRON_BATCH_SIZE ||
          100
      )
    );
    cron.schedule(
      cleanupExpr,
      () => {
        void (async () => {
          const missing = validateEnv();
          if (missing.length > 0) {
            logCleanupError("Scheduled shoot cleanup skipped: missing env", {
              missing,
            });
            return;
          }
          const runId = `cron-cleanup-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
          try {
            await runStaleShootCleanupJob({
              deleteBatchSize: cleanupBatchSize,
              runId,
              source: "cron",
            });
          } catch (error: unknown) {
            if (axios.isAxiosError(error)) {
              logCleanupError("Cron shoot cleanup: Wix API failed", {
                runId,
                status: error.response?.status,
                details: error.response?.data || error.message,
              });
              return;
            }
            logCleanupError("Cron shoot cleanup failed", {
              runId,
              details: error instanceof Error ? error.message : "Unknown error",
            });
          }
        })();
      },
      cronOptions
    );
    logCleanupInfo("Scheduled shoot-date cleanup cron active", {
      expression: cleanupExpr,
      timezone: cronTz ?? "server local time",
      batchSize: cleanupBatchSize,
    });
  } else {
    logCleanupInfo(
      "Shoot-date cleanup cron disabled (CRON_CLEANUP_EXPRESSION=off). Empty = default 03:30 daily."
    );
  }
});


process.stdin.resume();

const shutdown = (signal: string): void => {
  console.log(`${signal} received, shutting down server...`);
  server.close(() => {
    process.exit(0);
  });
};

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
