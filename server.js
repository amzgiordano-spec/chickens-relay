// Chickens in Hats — servidor de salas (relay)
// ------------------------------------------------------------
// Conecta a los jugadores entre sí para jugar en línea desde cualquier lugar
// (navegador, celular o Windows) sin tocar el router.
//
//   * El que crea la sala recibe un código de 5 letras (ej. "KQZPM").
//   * Los demás se unen con ese código.
//   * El servidor solo reenvía los mensajes entre el host y los jugadores.
//
// No necesita instalar nada: usa solo lo que trae Node.js (versión 18 o más nueva).
// Para correrlo:  node server.js      (usa el puerto de la variable PORT, o 8080)

const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ";
const MAX_PLAYERS = 6;       // salas de 4 o de 6 pollos (lo elige el que crea la sala)
const MAX_MESSAGE = 256 * 1024;

const rooms = new Map(); // código -> { host, peers: Map(id -> conn) }
let connections = 0;

// ------------------------------------------------------------ HTTP (para ver que anda)
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
  res.end(`Chickens in Hats relay is running! Rooms: ${rooms.size}, players: ${connections}\n`);
});

// ------------------------------------------------------------ WebSocket (hecho a mano, sin librerías)
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);
  const conn = new Conn(socket);
  connections++;
  socket.on("close", () => { connections--; conn.onClose(); });
  socket.on("error", () => socket.destroy());
});

class Conn {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragOp = 0;
    this.room = null;
    this.id = 0;
    this.alive = true;
    this.closed = false;
    socket.on("data", (d) => this.onData(d));
  }

  onData(data) {
    this.buf = Buffer.concat([this.buf, data]);
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MESSAGE) return this.socket.destroy();
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = this.buf.subarray(off, off + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      this.buf = this.buf.subarray(need);
      if (op === 0x8) { this.close(); return; }             // close
      if (op === 0x9) { this.sendFrame(0xA, payload); continue; } // ping -> pong
      if (op === 0xA) { this.alive = true; continue; }       // pong
      if (op === 0x0) {                                       // continuación
        this.frags.push(payload);
        if (fin) { this.onMessage(this.fragOp, Buffer.concat(this.frags)); this.frags = []; }
        continue;
      }
      if (!fin) { this.fragOp = op; this.frags = [payload]; continue; }
      this.onMessage(op, payload);
    }
  }

  sendFrame(op, payload) {
    if (this.closed) return;
    const len = payload.length;
    let header;
    if (len < 126) { header = Buffer.from([0x80 | op, len]); }
    else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | op; header[1] = 126; header.writeUInt16BE(len, 2); }
    else { header = Buffer.alloc(10); header[0] = 0x80 | op; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
    this.socket.write(Buffer.concat([header, payload]));
  }

  sendJSON(obj) { this.sendFrame(0x1, Buffer.from(JSON.stringify(obj))); }

  close() {
    if (this.closed) return;
    try { this.sendFrame(0x8, Buffer.alloc(0)); } catch (e) {}
    this.closed = true;
    this.socket.end();
    this.onClose();
  }

  // ---------------------------------------------------------- lógica de salas
  onMessage(op, data) {
    if (op === 0x1) {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch (e) { return; }
      this.onCommand(msg);
    } else if (op === 0x2) {
      this.onGameData(data);
    }
  }

  onCommand(msg) {
    if (msg.op === "create" && !this.room) {
      let code;
      do { code = Array.from({ length: 5 }, () => CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]).join(""); }
      while (rooms.has(code));
      const max = Math.max(2, Math.min(MAX_PLAYERS, Number(msg.max) || 4));
      const room = { code, host: this, peers: new Map([[1, this]]), open: true, max };
      rooms.set(code, room);
      this.room = room;
      this.id = 1;
      this.sendJSON({ op: "created", code, id: 1 });
      log(`room ${code} created (${max} chickens)`);
    } else if (msg.op === "join" && !this.room) {
      const code = String(msg.code || "").toUpperCase().replace(/[^A-Z]/g, "");
      const room = rooms.get(code);
      if (!room) return this.sendJSON({ op: "error", msg: "No room with that code." });
      if (!room.open) return this.sendJSON({ op: "error", msg: "That room is already playing. Try again in a moment!" });
      if (room.peers.size >= room.max) return this.sendJSON({ op: "error", msg: "That room is full." });
      let id;
      do { id = 2 + crypto.randomInt(2 ** 30); } while (room.peers.has(id));
      room.peers.set(id, this);
      this.room = room;
      this.id = id;
      this.sendJSON({ op: "joined", code, id });
      room.host.sendJSON({ op: "peer_join", id });
      log(`room ${code}: player joined (${room.peers.size}/${room.max})`);
    } else if (msg.op === "open" && this.room && this.id === 1) {
      this.room.open = !!msg.value;            // el host avisa si se puede entrar (sala de espera) o no (jugando)
    } else if (msg.op === "kick" && this.room && this.id === 1) {
      const p = this.room.peers.get(msg.id);
      if (p && p !== this) { p.sendJSON({ op: "error", msg: msg.reason || "Removed from the room." }); p.close(); }
    }
  }

  // Datos del juego: [4 bytes destino][datos]  ->  se reenvía como [4 bytes origen][datos]
  onGameData(data) {
    const room = this.room;
    if (!room || data.length < 4) return;
    const target = data.readInt32LE(0);
    const out = Buffer.from(data);
    out.writeInt32LE(this.id, 0);
    if (target > 0) {
      const p = room.peers.get(target);
      if (p && p !== this) p.sendFrame(0x2, out);
    } else {
      for (const [pid, p] of room.peers) {
        if (p === this || (target < 0 && pid === -target)) continue;
        p.sendFrame(0x2, out);
      }
    }
  }

  onClose() {
    this.closed = true;
    const room = this.room;
    if (!room) return;
    this.room = null;
    if (room.host === this) {
      rooms.delete(room.code);
      for (const [pid, p] of room.peers) {
        if (p !== this) { p.sendJSON({ op: "host_left" }); p.close(); }
      }
      log(`room ${room.code} closed`);
    } else {
      room.peers.delete(this.id);
      room.host.sendJSON({ op: "peer_leave", id: this.id });
    }
  }
}

// Ping cada 25 s para mantener viva la conexión y limpiar las que se cortaron.
setInterval(() => {
  for (const room of rooms.values()) {
    for (const p of room.peers.values()) {
      if (!p.alive) { p.socket.destroy(); continue; }
      p.alive = false;
      p.sendFrame(0x9, Buffer.alloc(0));
    }
  }
}, 25000);

function log(s) { console.log(new Date().toISOString().slice(11, 19), s); }

server.listen(PORT, () => log(`Chickens in Hats relay listening on port ${PORT}`));
