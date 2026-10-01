const test = require("node:test");
const assert = require("node:assert/strict");

const bridge = require("../api/telegram.js")._test;

test("parses execute callback", () => {
  const parsed = bridge.parseDoneCallback(
    "rrd|BOOK_2|ALGO|FIL|20260928|10723.76037691"
  );
  assert.deepEqual(parsed, {
    bookId: "BOOK_2",
    fromAsset: "ALGO",
    toAsset: "FIL",
    signalDate: "20260928",
    sourceQuantity: "10723.76037691"
  });
});

test("marker round trip", () => {
  const signal = {
    bookId: "BOOK_1",
    fromAsset: "ATOM",
    toAsset: "AVAX",
    signalDate: "20260930",
    sourceQuantity: "-"
  };
  const marker = bridge.markerFor(signal);
  assert.deepEqual(bridge.parseExecutionMarker(marker), signal);
});

test("known source quantity accepts one received quantity", () => {
  assert.deepEqual(
    bridge.parseExecutionQuantities("1234,56", "81"),
    { sentQuantity: 81, receivedQuantity: 1234.56 }
  );
});

test("known source quantity can be overridden with two quantities", () => {
  assert.deepEqual(
    bridge.parseExecutionQuantities("80.5 1234.56", "81"),
    { sentQuantity: 80.5, receivedQuantity: 1234.56 }
  );
});

test("unknown source quantity requires sent and received", () => {
  assert.equal(bridge.parseExecutionQuantities("1234.56", "-"), null);
  assert.deepEqual(
    bridge.parseExecutionQuantities("81 1234.56", "-"),
    { sentQuantity: 81, receivedQuantity: 1234.56 }
  );
});

test("rejects malformed date and quantity", () => {
  assert.throws(() =>
    bridge.parseDoneCallback("rrd|BOOK_2|ALGO|FIL|20261350|10")
  );
  assert.equal(bridge.parseExecutionQuantities("-1 5", "-"), null);
});


test("parses missed-morning Telegram commands", () => {
  assert.deepEqual(bridge.parseMissedCommand("/missed BOOK_2"), {
    bookId: "BOOK_2"
  });
  assert.deepEqual(bridge.parseMissedCommand("пропустил утренний сигнал BOOK_1"), {
    bookId: "BOOK_1"
  });
  assert.deepEqual(bridge.parseMissedCommand("пропустил сигнал"), {
    bookId: ""
  });
  assert.equal(bridge.parseMissedCommand("что-то другое"), null);
});

test("parses Telegram position sync commands", () => {
  assert.deepEqual(
    bridge.parsePositionCommand("/position BOOK_2 TRX 3950.7453"),
    { bookId: "BOOK_2", asset: "TRX", quantity: 3950.7453 }
  );
  assert.deepEqual(
    bridge.parsePositionCommand("позиция BOOK_1 ATOM 81,5"),
    { bookId: "BOOK_1", asset: "ATOM", quantity: 81.5 }
  );
  assert.deepEqual(
    bridge.parsePositionCommand("ротация BOOK_2 FIL 777"),
    { bookId: "BOOK_2", asset: "FIL", quantity: 777 }
  );
  assert.equal(bridge.parsePositionCommand("/position BOOK_2 TRX -1"), null);
});


test("builds persistent RR main menu", () => {
  const markup = bridge.mainMenuReplyMarkup();
  assert.equal(markup.is_persistent, true);
  assert.equal(markup.resize_keyboard, true);
  assert.deepEqual(
    markup.keyboard.flat().map((item) => item.text),
    [
      "📊 Мои позиции",
      "📡 Статус RR",
      "⏰ Пропустил сигнал",
      "✅ Выполнил ротацию"
    ]
  );
});

test("parses RR main menu actions", () => {
  assert.equal(bridge.parseMenuAction("/menu"), "MENU");
  assert.equal(bridge.parseMenuAction("/start"), "MENU");
  assert.equal(bridge.parseMenuAction("📊 Мои позиции"), "POSITIONS");
  assert.equal(bridge.parseMenuAction("📡 Статус RR"), "STATUS");
  assert.equal(bridge.parseMenuAction("⏰ Пропустил сигнал"), "MISSED");
  assert.equal(bridge.parseMenuAction("✅ Выполнил ротацию"), "EXECUTION");
  assert.equal(bridge.parseMenuAction("hello"), null);
});

test("position correction marker round trip", () => {
  assert.deepEqual(bridge.parsePositionEditCallback("rrp|BOOK_2"), {
    bookId: "BOOK_2"
  });
  const marker = bridge.positionMarkerFor("BOOK_2");
  assert.equal(marker, "[RR_POS|BOOK_2]");
  assert.deepEqual(bridge.parsePositionMarker(marker), { bookId: "BOOK_2" });
});

test("parses nested position correction reply", () => {
  assert.deepEqual(bridge.parsePositionReply("TRX 3950.7453"), {
    asset: "TRX",
    quantity: 3950.7453
  });
  assert.deepEqual(bridge.parsePositionReply("atom 81,5"), {
    asset: "ATOM",
    quantity: 81.5
  });
  assert.equal(bridge.parsePositionReply("TRX"), null);
  assert.equal(bridge.parsePositionReply("TRX -1"), null);
});
