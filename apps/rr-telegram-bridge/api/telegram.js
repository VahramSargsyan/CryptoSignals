const DEFAULT_REPOSITORY = "VahramSargsyan/CryptoSignals";
const DEFAULT_WORKFLOW = "relative-rotation-telegram-control-v1.yml";
const DEFAULT_OPERATIONS_WORKFLOW = "relative-rotation-telegram-ops-v1.yml";
const DEFAULT_MENU_WORKFLOW = "relative-rotation-telegram-menu-v1.yml";

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function isAllowedActor(update) {
  const configured = String(process.env.TELEGRAM_ALLOWED_USER_ID || "").trim();
  if (!configured) return true;

  const actor =
    update?.callback_query?.from?.id ??
    update?.message?.from?.id ??
    update?.edited_message?.from?.id;
  return String(actor || "") === configured;
}

function validWebhookSecret(request) {
  const expected = String(process.env.TELEGRAM_WEBHOOK_SECRET || "").trim();
  if (!expected) return false;
  const actual = String(
    request.headers["x-telegram-bot-api-secret-token"] || ""
  ).trim();
  return actual === expected;
}

function normalizeAsset(value) {
  const asset = String(value || "").trim().toUpperCase();
  if (!/^[A-Z0-9]{2,12}$/.test(asset)) {
    throw new Error("Invalid asset");
  }
  return asset;
}

function normalizeBookId(value) {
  const book = String(value || "").trim().toUpperCase();
  if (!/^BOOK_[1-9][0-9]*$/.test(book)) {
    throw new Error("Invalid book_id");
  }
  return book;
}

function normalizeSignalDate(value) {
  const text = String(value || "").trim();
  if (!/^20[0-9]{6}$/.test(text)) {
    throw new Error("Invalid signal date");
  }
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(4, 6));
  const day = Number(text.slice(6, 8));
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new Error("Invalid signal date");
  }
  return text;
}

function normalizeQuantityToken(value) {
  const text = String(value || "").trim();
  if (text === "-" || text === "") return "-";
  const normalized = text.replace(",", ".");
  const number = Number(normalized);
  if (!Number.isFinite(number) || number <= 0) {
    throw new Error("Invalid source quantity");
  }
  return String(number);
}

function parseDoneCallback(data) {
  const parts = String(data || "").split("|");
  if (parts.length !== 6 || parts[0] !== "rrd") {
    return null;
  }
  return {
    bookId: normalizeBookId(parts[1]),
    fromAsset: normalizeAsset(parts[2]),
    toAsset: normalizeAsset(parts[3]),
    signalDate: normalizeSignalDate(parts[4]),
    sourceQuantity: normalizeQuantityToken(parts[5])
  };
}

function parseLaterCallback(data) {
  const parts = String(data || "").split("|");
  if (parts.length < 2 || parts[0] !== "rrl") return null;
  return { bookId: normalizeBookId(parts[1]) };
}

function parseMissedCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(
    /^(?:\/missed(?:@[A-Za-z0-9_]+)?|пропустил(?:\s+утренний)?(?:\s+сигнал)?)(?:\s+(BOOK_[1-9][0-9]*))?$/iu
  );
  if (!match) return null;
  return { bookId: match[1] ? normalizeBookId(match[1]) : "" };
}

function parsePositionCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(
    /^(?:\/position(?:@[A-Za-z0-9_]+)?|позиция|ротация|запиши\s+позицию)\s+(BOOK_[1-9][0-9]*)\s+([A-Z0-9]{2,12})\s+([0-9]+(?:[.,][0-9]+)?)$/iu
  );
  if (!match) return null;
  const quantity = parsePositiveNumber(match[3]);
  if (quantity === null) return null;
  return {
    bookId: normalizeBookId(match[1]),
    asset: normalizeAsset(match[2]),
    quantity
  };
}

const MAIN_MENU_ACTIONS = new Map([
  ["📊 Мои позиции", "POSITIONS"],
  ["📡 Статус RR", "STATUS"],
  ["⏰ Пропустил сигнал", "MISSED"],
  ["✅ Выполнил ротацию", "EXECUTION"]
]);

function mainMenuReplyMarkup() {
  return {
    keyboard: [
      [
        { text: "📊 Мои позиции" },
        { text: "📡 Статус RR" }
      ],
      [
        { text: "⏰ Пропустил сигнал" },
        { text: "✅ Выполнил ротацию" }
      ]
    ],
    resize_keyboard: true,
    is_persistent: true,
    one_time_keyboard: false,
    input_field_placeholder: "Выбери действие RR"
  };
}

function parseMenuAction(text) {
  const raw = String(text || "").trim();
  if (/^\/(?:start|menu)(?:@[A-Za-z0-9_]+)?$/u.test(raw)) {
    return "MENU";
  }
  return MAIN_MENU_ACTIONS.get(raw) || null;
}

function parsePositionEditCallback(data) {
  const parts = String(data || "").split("|");
  if (parts.length !== 2 || parts[0] !== "rrp") return null;
  return { bookId: normalizeBookId(parts[1]) };
}

function positionMarkerFor(bookId) {
  return `[RR_POS|${normalizeBookId(bookId)}]`;
}

function parsePositionMarker(text) {
  const match = String(text || "").match(/\[RR_POS\|(BOOK_[1-9][0-9]*)\]/);
  if (!match) return null;
  return { bookId: normalizeBookId(match[1]) };
}

function parsePositionReply(text) {
  const match = String(text || "")
    .trim()
    .match(/^([A-Z0-9]{2,12})\s+([0-9]+(?:[.,][0-9]+)?)$/iu);
  if (!match) return null;
  const quantity = parsePositiveNumber(match[2]);
  if (quantity === null) return null;
  return {
    asset: normalizeAsset(match[1]),
    quantity
  };
}

function markerFor(signal) {
  return [
    "[RR_EXEC",
    signal.bookId,
    signal.fromAsset,
    signal.toAsset,
    signal.signalDate,
    signal.sourceQuantity
  ].join("|") + "]";
}

function parseExecutionMarker(text) {
  const match = String(text || "").match(
    /\[RR_EXEC\|(BOOK_[1-9][0-9]*)\|([A-Z0-9]{2,12})\|([A-Z0-9]{2,12})\|(20[0-9]{6})\|([^\]]+)\]/
  );
  if (!match) return null;
  return {
    bookId: normalizeBookId(match[1]),
    fromAsset: normalizeAsset(match[2]),
    toAsset: normalizeAsset(match[3]),
    signalDate: normalizeSignalDate(match[4]),
    sourceQuantity: normalizeQuantityToken(match[5])
  };
}

function parsePositiveNumber(value) {
  const normalized = String(value || "").trim().replace(",", ".");
  if (!/^[0-9]+(?:\.[0-9]+)?$/.test(normalized)) return null;
  const number = Number(normalized);
  if (!Number.isFinite(number) || number <= 0) return null;
  return number;
}

function parseExecutionQuantities(text, sourceQuantity) {
  const tokens = String(text || "")
    .trim()
    .split(/[\s;]+/)
    .filter(Boolean);

  if (sourceQuantity !== "-") {
    if (tokens.length === 1) {
      const received = parsePositiveNumber(tokens[0]);
      if (received === null) return null;
      return {
        sentQuantity: Number(sourceQuantity),
        receivedQuantity: received
      };
    }
    if (tokens.length === 2) {
      const sent = parsePositiveNumber(tokens[0]);
      const received = parsePositiveNumber(tokens[1]);
      if (sent === null || received === null) return null;
      return { sentQuantity: sent, receivedQuantity: received };
    }
    return null;
  }

  if (tokens.length !== 2) return null;
  const sent = parsePositiveNumber(tokens[0]);
  const received = parsePositiveNumber(tokens[1]);
  if (sent === null || received === null) return null;
  return { sentQuantity: sent, receivedQuantity: received };
}

async function telegramCall(method, payload) {
  const token = requiredEnv("TELEGRAM_BOT_TOKEN");
  const response = await fetch(
    `https://api.telegram.org/bot${token}/${method}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    }
  );
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.ok === false) {
    throw new Error(
      `Telegram ${method} failed: HTTP ${response.status} ${JSON.stringify(body)}`
    );
  }
  return body;
}

async function dispatchExecution(signal, quantities, update) {
  const token = requiredEnv("GITHUB_DISPATCH_TOKEN");
  const repository = String(
    process.env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY
  ).trim();
  const workflow = String(
    process.env.GITHUB_CONTROL_WORKFLOW || DEFAULT_WORKFLOW
  ).trim();

  const executedAt = new Date(
    Number(update.message.date) * 1000
  ).toISOString();

  const response = await fetch(
    `https://api.github.com/repos/${repository}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2026-03-10",
        "content-type": "application/json",
        "user-agent": "rr-telegram-control-bridge"
      },
      body: JSON.stringify({
        ref: "main",
        inputs: {
          action: "CONFIRM_EXECUTION",
          book_id: signal.bookId,
          signal_date: signal.signalDate,
          from_asset: signal.fromAsset,
          to_asset: signal.toAsset,
          sent_quantity: String(quantities.sentQuantity),
          received_quantity: String(quantities.receivedQuantity),
          confirmed_at: executedAt,
          telegram_update_id: String(update.update_id ?? "")
        }
      })
    }
  );

  if (![200, 204].includes(response.status)) {
    const body = await response.text();
    throw new Error(
      `GitHub workflow dispatch failed: HTTP ${response.status} ${body}`
    );
  }
}

function operationTimestamp(update) {
  const unixSeconds = update?.message?.date;
  if (Number.isFinite(Number(unixSeconds)) && Number(unixSeconds) > 0) {
    return new Date(Number(unixSeconds) * 1000).toISOString();
  }
  return new Date().toISOString();
}

async function dispatchOperation(action, payload, update) {
  const token = requiredEnv("GITHUB_DISPATCH_TOKEN");
  const repository = String(
    process.env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY
  ).trim();
  const workflow = String(
    process.env.GITHUB_OPERATIONS_WORKFLOW || DEFAULT_OPERATIONS_WORKFLOW
  ).trim();

  const response = await fetch(
    `https://api.github.com/repos/${repository}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2022-11-28",
        "content-type": "application/json",
        "user-agent": "rr-telegram-control-bridge"
      },
      body: JSON.stringify({
        ref: "main",
        inputs: {
          action,
          book_id: String(payload.bookId || ""),
          asset: String(payload.asset || ""),
          quantity:
            payload.quantity === undefined || payload.quantity === null
              ? ""
              : String(payload.quantity),
          confirmed_at: operationTimestamp(update),
          telegram_update_id: String(update.update_id ?? "")
        }
      })
    }
  );

  if (![200, 204].includes(response.status)) {
    const body = await response.text();
    throw new Error(
      `GitHub operations dispatch failed: HTTP ${response.status} ${body}`
    );
  }
}

async function dispatchMenu(action) {
  const token = requiredEnv("GITHUB_DISPATCH_TOKEN");
  const repository = String(
    process.env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY
  ).trim();
  const workflow = String(
    process.env.GITHUB_MENU_WORKFLOW || DEFAULT_MENU_WORKFLOW
  ).trim();

  const response = await fetch(
    `https://api.github.com/repos/${repository}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2022-11-28",
        "content-type": "application/json",
        "user-agent": "rr-telegram-control-bridge"
      },
      body: JSON.stringify({
        ref: "main",
        inputs: { action }
      })
    }
  );

  if (![200, 204].includes(response.status)) {
    const body = await response.text();
    throw new Error(
      `GitHub menu workflow dispatch failed: HTTP ${response.status} ${body}`
    );
  }
}

async function handleCallback(update) {
  const query = update.callback_query;
  const done = parseDoneCallback(query.data);
  if (done) {
    const chatId = query.message?.chat?.id;
    if (!chatId) throw new Error("Callback has no chat id");

    await telegramCall("answerCallbackQuery", {
      callback_query_id: query.id,
      text: "Укажи фактическое количество после обмена."
    });

    const marker = markerFor(done);
    const known = done.sourceQuantity !== "-";
    const prompt = known
      ? [
          `✅ ${done.bookId}: ${done.fromAsset} → ${done.toAsset}`,
          `Зафиксировано к отправке: ${done.sourceQuantity} ${done.fromAsset}.`,
          `Ответь ОДНИМ числом — сколько ${done.toAsset} фактически получил.`,
          "Если количество отправленного отличалось, ответь двумя числами: ОТДАЛ ПОЛУЧИЛ.",
          marker
        ].join("\n")
      : [
          `✅ ${done.bookId}: ${done.fromAsset} → ${done.toAsset}`,
          `Ответь ДВУМЯ числами: сколько ${done.fromAsset} отдал и сколько ${done.toAsset} получил.`,
          "Пример: 81 1234.56",
          marker
        ].join("\n");

    await telegramCall("sendMessage", {
      chat_id: chatId,
      text: prompt,
      reply_markup: { force_reply: true, selective: true }
    });
    return;
  }

  const later = parseLaterCallback(query.data);
  if (later) {
    await telegramCall("answerCallbackQuery", {
      callback_query_id: query.id,
      text: `${later.bookId}: отмечаю пропущенный утренний сигнал.`
    });
    await dispatchOperation(
      "MARK_MORNING_MISSED",
      { bookId: later.bookId },
      update
    );
    const chatId = query.message?.chat?.id;
    if (chatId) {
      await telegramCall("sendMessage", {
        chat_id: chatId,
        text: [
          `⏳ ${later.bookId}: передал отметку о пропущенном утреннем сигнале в GitHub.`,
          "После проверки GitHub включит вечерний этап на 22:30 по Еревану."
        ].join("\n")
      });
    }
    return;
  }

  const editPosition = parsePositionEditCallback(query.data);
  if (editPosition) {
    const chatId = query.message?.chat?.id;
    if (!chatId) throw new Error("Callback has no chat id");

    await telegramCall("answerCallbackQuery", {
      callback_query_id: query.id,
      text: `${editPosition.bookId}: укажи фактическую позицию.`
    });

    await telegramCall("sendMessage", {
      chat_id: chatId,
      text: [
        `✏️ ${editPosition.bookId}: исправление текущей позиции`,
        "Ответь: АКТИВ КОЛИЧЕСТВО",
        "Пример: TRX 3950.7453",
        positionMarkerFor(editPosition.bookId)
      ].join("\n"),
      reply_markup: { force_reply: true, selective: true }
    });
    return;
  }

  await telegramCall("answerCallbackQuery", {
    callback_query_id: query.id,
    text: "Неизвестная команда.",
    show_alert: true
  });
}

async function handleExecutionReply(update) {
  const message = update.message;
  const reply = message?.reply_to_message;
  if (!reply?.from?.is_bot) return false;

  const signal = parseExecutionMarker(reply.text || "");
  if (!signal) return false;

  const quantities = parseExecutionQuantities(
    message.text || "",
    signal.sourceQuantity
  );
  if (!quantities) {
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text:
        signal.sourceQuantity === "-"
          ? "Неверный формат. Ответь двумя положительными числами: ОТДАЛ ПОЛУЧИЛ."
          : "Неверный формат. Ответь количеством полученного актива одним числом, либо двумя числами ОТДАЛ ПОЛУЧИЛ.",
      reply_to_message_id: message.message_id
    });
    return true;
  }

  await dispatchExecution(signal, quantities, update);
  await telegramCall("sendMessage", {
    chat_id: message.chat.id,
    text: [
      "⏳ Передал подтверждение в GitHub.",
      `${signal.bookId}: ${quantities.sentQuantity} ${signal.fromAsset} → ${quantities.receivedQuantity} ${signal.toAsset}`,
      "GitHub ещё раз проверит текущий BOOK и CONFIRMED-сигнал перед записью.",
      "После успешной проверки придёт отдельное подтверждение."
    ].join("\n"),
    reply_to_message_id: message.message_id
  });
  return true;
}

async function handlePositionReply(update) {
  const message = update.message;
  const reply = message?.reply_to_message;
  if (!reply?.from?.is_bot) return false;

  const marker = parsePositionMarker(reply.text || "");
  if (!marker) return false;

  const parsed = parsePositionReply(message.text || "");
  if (!parsed) {
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text: "Неверный формат. Ответь: АКТИВ КОЛИЧЕСТВО. Пример: TRX 3950.7453",
      reply_to_message_id: message.message_id
    });
    return true;
  }

  await dispatchOperation(
    "SET_POSITION",
    {
      bookId: marker.bookId,
      asset: parsed.asset,
      quantity: parsed.quantity
    },
    update
  );
  await telegramCall("sendMessage", {
    chat_id: message.chat.id,
    text: [
      "⏳ Передал исправление позиции в GitHub.",
      `${marker.bookId}: ${parsed.quantity} ${parsed.asset}`,
      "GitHub проверит допустимость актива перед записью."
    ].join("\n"),
    reply_to_message_id: message.message_id
  });
  return true;
}

async function handleTextCommand(update) {
  const message = update.message;
  const text = String(message?.text || "").trim();
  if (!text) return false;

  const menuAction = parseMenuAction(text);
  if (menuAction === "MENU") {
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text: [
        "Relative Rotation — главное меню",
        "📡 «Статус RR» пересчитывает актуальное состояние по последней закрытой D1-свече.",
        "✅ и ⏰ становятся действиями только после проверки текущего CONFIRMED."
      ].join("\n"),
      reply_markup: mainMenuReplyMarkup()
    });
    return true;
  }

  if (menuAction) {
    await dispatchMenu(menuAction);
    const messages = {
      POSITIONS: "⏳ Получаю канонические позиции из GitHub…",
      STATUS: "⏳ Пересчитываю актуальный статус Relative Rotation…",
      MISSED: "⏳ Проверяю, какой текущий CONFIRMED можно отметить как пропущенный…",
      EXECUTION: "⏳ Проверяю текущую CONFIRMED-ротацию для записи исполнения…"
    };
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text: messages[menuAction],
      reply_markup: mainMenuReplyMarkup()
    });
    return true;
  }

  const missed = parseMissedCommand(text);
  if (missed) {
    await dispatchOperation("MARK_MORNING_MISSED", missed, update);
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text: [
        "⏳ Передал отметку о пропущенном утреннем сигнале в GitHub.",
        missed.bookId
          ? `BOOK: ${missed.bookId}`
          : "BOOK будет определён по единственному актуальному CONFIRMED.",
        "После проверки GitHub включит вечерний этап на 22:30 по Еревану."
      ].join("\n"),
      reply_to_message_id: message.message_id
    });
    return true;
  }

  const position = parsePositionCommand(text);
  if (position) {
    await dispatchOperation("SET_POSITION", position, update);
    await telegramCall("sendMessage", {
      chat_id: message.chat.id,
      text: [
        "⏳ Передал текущую позицию в GitHub.",
        `${position.bookId}: ${position.quantity} ${position.asset}`,
        "GitHub запишет фактическое состояние без создания биржевого ордера."
      ].join("\n"),
      reply_to_message_id: message.message_id
    });
    return true;
  }

  return false;
}

async function handler(request, response) {
  if (request.method === "GET") {
    response.status(200).json({
      ok: true,
      service: "rr-telegram-control-bridge",
      webhook: "ready"
    });
    return;
  }

  if (request.method !== "POST") {
    response.setHeader("allow", "GET, POST");
    response.status(405).json({ ok: false, error: "method_not_allowed" });
    return;
  }

  if (!validWebhookSecret(request)) {
    response.status(401).json({ ok: false, error: "invalid_webhook_secret" });
    return;
  }

  const update = request.body || {};
  if (!isAllowedActor(update)) {
    response.status(403).json({ ok: false, error: "actor_not_allowed" });
    return;
  }

  try {
    if (update.callback_query) {
      await handleCallback(update);
    } else if (update.message) {
      const handledExecution = await handleExecutionReply(update);
      if (!handledExecution) {
        const handledPosition = await handlePositionReply(update);
        if (!handledPosition) {
          await handleTextCommand(update);
        }
      }
    }
    response.status(200).json({ ok: true });
  } catch (error) {
    console.error(error);
    try {
      const chatId =
        update?.callback_query?.message?.chat?.id ?? update?.message?.chat?.id;
      if (chatId) {
        await telegramCall("sendMessage", {
          chat_id: chatId,
          text: "❌ Не удалось передать действие в GitHub. Позиция не изменена. Попробуй позже или сообщи в ChatGPT."
        });
      }
    } catch (secondary) {
      console.error(secondary);
    }
    response.status(500).json({ ok: false, error: "bridge_failure" });
  }
}

module.exports = handler;
module.exports._test = {
  markerFor,
  mainMenuReplyMarkup,
  markerFor,
  parseDoneCallback,
  parseMissedCommand,
  parsePositionCommand,
  parseMenuAction,
  parsePositionEditCallback,
  positionMarkerFor,
  parsePositionMarker,
  parsePositionReply,
  parseExecutionMarker,
  parseExecutionQuantities,
  normalizeSignalDate,
  normalizeQuantityToken,
  operationTimestamp,
  dispatchOperation,
  dispatchMenu
};
