module.exports = function handler(_request, response) {
  response.status(200).json({
    ok: true,
    service: "rr-telegram-control-bridge",
    version: "1.0.0"
  });
};
