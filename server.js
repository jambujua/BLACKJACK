const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static(__dirname));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://deaagus785_db_user:yXeBKToNEuqQ7au5@cluster0.uh7hryx.mongodb.net/?appName=Cluster0";
mongoose.connect(MONGO_URI)
  .then(() => console.log("Terhubung ke Database MongoDB!"))
  .catch(err => console.error("Koneksi database gagal:", err));

const playerSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  balance: { type: Number, default: 0 },
  status: { type: String, enum: ['pending', 'active', 'banned'], default: 'pending' },
  topupRequested: { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now }
});
const Player = mongoose.model('Player', playerSchema);

const historySchema = new mongoose.Schema({
  username: { type: String, required: true },
  roomNumber: { type: String, required: true },
  result: { type: String, enum: ['WIN', 'LOSE'], required: true },
  pointsChange: { type: Number, required: true },
  durationMinutes: { type: Number, default: 0 },
  playedAt: { type: Date, default: Date.now }
});
const History = mongoose.model('History', historySchema);

// --- API ROUTES ---
app.post('/api/register', async (req, res) => {
  try {
    const { username, password } = req.body;
    const existing = await Player.findOne({ username });
    if (existing) return res.status(400).json({ message: "Username sudah digunakan!" });
    const newPlayer = new Player({ username, password, balance: 0, status: 'pending', topupRequested: false });
    await newPlayer.save();
    res.json({ success: true, message: "Pendaftaran berhasil! Tunggu ACC Admin." });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    const player = await Player.findOne({ username, password });
    if (!player) return res.status(400).json({ message: "Username atau password salah!" });
    if (player.status === 'pending') return res.status(403).json({ message: "Akun belum di-ACC Admin." });
    res.json({ success: true, player });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/request-topup', async (req, res) => {
  try {
    const { username } = req.body;
    await Player.findOneAndUpdate({ username }, { topupRequested: true }, { returnDocument: 'after' });
    res.json({ success: true, message: "Permintaan koin berhasil dikirim ke Admin." });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/admin/all-players', async (req, res) => {
  try {
    const players = await Player.find().sort({ createdAt: -1 });
    res.json(players);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/admin/acc-player', async (req, res) => {
  try {
    const { playerId } = req.body;
    const player = await Player.findByIdAndUpdate(playerId, { status: 'active', balance: 100000, topupRequested: false }, { returnDocument: 'after' });
    res.json({ success: true, message: `Akun ${player.username} di-ACC!`, player });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/admin/update-balance', async (req, res) => {
  try {
    const { username, newBalance } = req.body;
    const player = await Player.findOneAndUpdate({ username }, { balance: Number(newBalance), topupRequested: false }, { returnDocument: 'after' });
    res.json({ success: true, player });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/admin/delete-player/:id', async (req, res) => {
  try {
    await Player.findByIdAndDelete(req.params.id);
    res.json({ success: true, message: "Akun dihapus." });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/history/save', async (req, res) => {
  try {
    await new History(req.body).save();
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/history', async (req, res) => {
  try {
    const { username } = req.query;
    let query = username ? { username } : {};
    const histories = await History.find(query).sort({ playedAt: -1 }).limit(100);
    res.json(histories);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// --- SOCKET.IO ROOMS & GAME ENGINE ---
const rooms = {};

async function savePlayerBalanceToDB(username, balance) {
  try {
    await Player.findOneAndUpdate({ username }, { balance: Number(balance) });
  } catch (e) {
    console.error("Gagal sinkronisasi saldo ke DB:", e.message);
  }
}

io.on('connection', (socket) => {
  socket.on('create_room', ({ roomId, username, balance }) => {
    if (rooms[roomId]) {
      socket.emit('room_error', 'Room ID sudah ada! Gunakan ID lain yang unik.');
      return;
    }
    rooms[roomId] = { 
      hostSocketId: socket.id,
      hostUsername: username,
      players: [], 
      dealerHand: [],
      deck: [],
      gameStarted: false,
      endingGame: false,
      endGameAgrees: [],
      currentTurnPlayerIndex: 0,
      currentTurnHandIndex: 0,
      dealerDone: false,
      lastActivityTime: Date.now()
    };
    socket.join(roomId);

    rooms[roomId].players.push({
      socketId: socket.id, username, balance, bet: 0, ready: true,
      hands: [], statuses: ['host'], splitCount: 0
    });

    socket.emit('room_joined', { roomId, isHost: true, hostUsername: username });
    io.to(roomId).emit('update_room_state', rooms[roomId]);
  });

  socket.on('join_room', ({ roomId, username, balance }) => {
    let room = rooms[roomId];
    if (!room) {
      socket.emit('room_error', 'Room tidak ditemukan!');
      return;
    }

    let existingPlayer = room.players.find(p => p.username === username);
    if (existingPlayer) {
      existingPlayer.socketId = socket.id;
      socket.join(roomId);
      let isHost = (room.hostSocketId === socket.id);
      socket.emit('room_joined', { roomId, isHost, hostUsername: room.hostUsername });
      socket.emit('update_room_state', room);
      return;
    }

    socket.join(roomId);
    room.players.push({ 
      socketId: socket.id, username, balance, bet: 1000, ready: false, 
      hands: [[]], statuses: ['waiting'], splitCount: 0 
    });
    room.lastActivityTime = Date.now();
    let isHost = (room.hostSocketId === socket.id);
    socket.emit('room_joined', { roomId, isHost, hostUsername: room.hostUsername });
    io.to(roomId).emit('update_room_state', room);
  });

  socket.on('set_ready', ({ roomId, bet, ready }) => {
    let room = rooms[roomId];
    if (!room) return;
    let p = room.players.find(x => x.socketId === socket.id);
    if (p && socket.id !== room.hostSocketId) {
      let numericBet = parseInt(bet) || 1000;
      if (numericBet > 10000000) numericBet = 10000000; // Batas maksimal 10.000.000
      p.bet = numericBet;
      p.ready = Boolean(ready);
      room.lastActivityTime = Date.now();
    }
    io.to(roomId).emit('update_room_state', room);
  });

  // Fitur Bandar Kick Pemain
  socket.on('kick_player', ({ roomId, usernameToKick }) => {
    let room = rooms[roomId];
    if (!room || room.hostSocketId !== socket.id) return;

    let targetPlayer = room.players.find(p => p.username === usernameToKick);
    if (targetPlayer) {
      savePlayerBalanceToDB(targetPlayer.username, targetPlayer.balance);
      io.to(targetPlayer.socketId).emit('kicked_from_room', 'Anda telah dikeluarkan oleh Bandar karena tidak ada respons.');
      room.players = room.players.filter(p => p.username !== usernameToKick);
      io.to(roomId).emit('update_room_state', room);
    }
  });

  // Fitur Bandar Paksa Akhiri Game (1 menit tanpa respons atau manual)
  socket.on('host_force_end_game', (roomId) => {
    let room = rooms[roomId];
    if (!room || room.hostSocketId !== socket.id) return;

    room.players.forEach(p => {
      savePlayerBalanceToDB(p.username, p.balance);
    });

    io.to(roomId).emit('game_ended_permanently', 'Permainan diakhiri oleh Bandar.');
    delete rooms[roomId];
  });

  socket.on('request_end_game', (roomId) => {
    let room = rooms[roomId];
    if (!room) return;
    if (!room.endGameAgrees) room.endGameAgrees = [];
    
    if (!room.endGameAgrees.includes(socket.id)) {
      room.endGameAgrees.push(socket.id);
    }

    let nonHostPlayers = room.players.filter(p => p.socketId !== room.hostSocketId);
    let hostPlayer = room.players.find(p => p.socketId === room.hostSocketId);
    let totalParticipants = nonHostPlayers.length + (hostPlayer ? 1 : 0);

    if (room.endGameAgrees.length >= totalParticipants) {
      room.players.forEach((p) => {
        savePlayerBalanceToDB(p.username, p.balance);
      });
      io.to(roomId).emit('game_ended_permanently', "Game diakhiri dan seluruh saldo telah tersimpan ke database.");
      delete rooms[roomId];
      return;
    }

    io.to(roomId).emit('update_room_state', room);
  });

  socket.on('start_game', (roomId) => {
    let room = rooms[roomId];
    if (room && room.hostSocketId === socket.id) {
      let bettingPlayers = room.players.filter(p => p.socketId !== room.hostSocketId);
      if (bettingPlayers.length === 0) {
        socket.emit('room_error', 'Tidak dapat mulai: Belum ada pemain lain di meja!');
        return;
      }
      
      room.gameStarted = true;
      room.dealerDone = false;
      room.deck = buildDeck();
      room.dealerHand = [];
      room.lastActivityTime = Date.now();

      room.players.forEach(p => {
        if (p.socketId !== room.hostSocketId) {
          p.hands = [[]];
          p.statuses = ['waiting'];
          p.splitCount = 0;
          p.roundResults = [];
          p.ready = false; 
        }
      });

      for (let round = 0; round < 2; round++) {
        room.players.forEach(p => {
          if (p.socketId !== room.hostSocketId) {
            p.hands[0].push(room.deck.pop());
          }
        });
        room.dealerHand.push(room.deck.pop());
      }

      let dScore = calculateScore(room.dealerHand);
      if (room.dealerHand.length === 2 && dScore === 21) {
        room.dealerDone = true;
        let hostPlayer = room.players.find(p => p.socketId === room.hostSocketId);
        
        room.players.forEach(p => {
          if (p.socketId !== room.hostSocketId) {
            p.statuses[0] = 'stand';
            p.roundResults = ['LOSE'];
            p.balance -= Number(p.bet);
            if (hostPlayer) hostPlayer.balance += Number(p.bet);
            savePlayerBalanceToDB(p.username, p.balance);
          }
        });
        if (hostPlayer) savePlayerBalanceToDB(hostPlayer.username, hostPlayer.balance);
        io.to(roomId).emit('round_ended', room);
        return;
      }

      room.currentTurnPlayerIndex = 0;
      room.currentTurnHandIndex = 0;
      checkInitialPlayerStatus(room);

      io.to(roomId).emit('game_started', room);
    }
  });

  socket.on('player_action', ({ roomId, handIndex, action }) => {
    let room = rooms[roomId];
    if (!room) return;
    room.lastActivityTime = Date.now();

    if (socket.id === room.hostSocketId) {
      if (action === 'dealer_hit') {
        let currentDScore = calculateScore(room.dealerHand);
        if (currentDScore >= 17) {
          socket.emit('room_error', 'Bandar sudah mencapai 17 atau lebih, tidak bisa Hit lagi!');
          return;
        }
        room.dealerHand.push(room.deck.pop());
        let newDScore = calculateScore(room.dealerHand);
        if (newDScore >= 17) {
          room.dealerDone = true;
          evaluateHostRound(room);
        } else {
          io.to(roomId).emit('update_room_state', room);
        }
      } else if (action === 'dealer_stand') {
        room.dealerDone = true;
        evaluateHostRound(room);
      }
      return;
    }

    let activePlayer = room.players[room.currentTurnPlayerIndex];
    if (!activePlayer || activePlayer.socketId !== socket.id) return;
    if (room.currentTurnHandIndex !== handIndex) return;
    if (activePlayer.statuses[handIndex] !== 'playing') return;

    if (action === 'hit') {
      activePlayer.hands[handIndex].push(room.deck.pop());
      let score = calculateScore(activePlayer.hands[handIndex]);
      if (score >= 21) {
        activePlayer.statuses[handIndex] = score === 21 ? 'stand' : 'bust';
        advanceTurn(room);
      } else {
        io.to(roomId).emit('update_room_state', room);
      }
    } else if (action === 'stand') {
      activePlayer.statuses[handIndex] = 'stand';
      advanceTurn(room);
    } else if (action === 'double') {
      activePlayer.bet *= 2;
      if (activePlayer.bet > 10000000) activePlayer.bet = 10000000;
      activePlayer.hands[handIndex].push(room.deck.pop());
      let score = calculateScore(activePlayer.hands[handIndex]);
      activePlayer.statuses[handIndex] = score > 21 ? 'bust' : 'stand';
      advanceTurn(room);
    } else if (action === 'split') {
      let hand = activePlayer.hands[handIndex];
      if (hand.length === 2 && getCardValue(hand[0]) === getCardValue(hand[1]) && activePlayer.splitCount < 4) {
        let card2 = hand.pop();
        activePlayer.hands.push([card2, room.deck.pop()]);
        hand.push(room.deck.pop());
        activePlayer.statuses.push('playing');
        activePlayer.splitCount++;
        io.to(roomId).emit('update_room_state', room);
      }
    }
  });

  socket.on('send_chat', ({ roomId, username, message }) => {
    let room = rooms[roomId];
    if (room) room.lastActivityTime = Date.now();
    io.to(roomId).emit('receive_chat', { username, message });
  });

  socket.on('disconnect', () => {
    // Tidak langsung hapus pemain agar saat reload halaman bisa terhubung ulang (reconnect)
  });
});

// Interval pengecekan otomatis: Jika 1 menit tidak ada aktivitas saat game belum mulai / macet, bandar bisa akhiri
setInterval(() => {
  const now = Date.now();
  for (let roomId in rooms) {
    let room = rooms[roomId];
    if (room && !room.gameStarted && room.lastActivityTime && (now - room.lastActivityTime > 60000)) {
      // Lebih dari 60 detik (1 menit) tanpa aktivitas
      io.to(roomId).emit('inactive_timeout_warning', 'Meja tidak aktif selama 1 menit.');
    }
  }
}, 10000);

function checkInitialPlayerStatus(room) {
  while (room.currentTurnPlayerIndex < room.players.length && room.players[room.currentTurnPlayerIndex].socketId === room.hostSocketId) {
    room.currentTurnPlayerIndex++;
  }

  let p = room.players[room.currentTurnPlayerIndex];
  let roomId = room.roomId || Object.keys(rooms).find(k => rooms[k] === room);

  if (!p) {
    room.dealerDone = true;
    io.to(roomId).emit('update_room_state', room);
    return;
  }

  let score = calculateScore(p.hands[0]);
  if (score >= 21) {
    p.statuses[0] = score === 21 ? 'stand' : 'bust';
    advanceTurn(room);
  } else {
    p.statuses[0] = 'playing';
    io.to(roomId).emit('update_room_state', room);
  }
}

function advanceTurn(room) {
  room.currentTurnPlayerIndex++;
  room.currentTurnHandIndex = 0;

  while (room.currentTurnPlayerIndex < room.players.length && room.players[room.currentTurnPlayerIndex].socketId === room.hostSocketId) {
    room.currentTurnPlayerIndex++;
  }

  let roomId = room.roomId || Object.keys(rooms).find(k => rooms[k] === room);

  if (room.currentTurnPlayerIndex < room.players.length) {
    let nextP = room.players[room.currentTurnPlayerIndex];
    let score = calculateScore(nextP.hands[0]);
    if (score >= 21) {
      nextP.statuses[0] = score === 21 ? 'stand' : 'bust';
      advanceTurn(room);
    } else {
      nextP.statuses[0] = 'playing';
      io.to(roomId).emit('update_room_state', room);
    }
  } else {
    room.dealerDone = true;
    io.to(roomId).emit('update_room_state', room);
    return;
  }
}

function buildDeck() {
  const values = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"];
  const types = ["C", "D", "H", "S"];
  let deck = [];
  for (let type of types) {
    for (let value of values) { deck.push(value + "-" + type); }
  }
  return deck.sort(() => Math.random() - 0.5);
}

function getCardValue(card) {
  let val = card.split("-")[0];
  if (val === "A") return 11;
  if (["J", "Q", "K"].includes(val)) return 10;
  return parseInt(val);
}

function calculateScore(hand) {
  let score = 0, aces = 0;
  for (let card of hand) {
    let val = card.split("-")[0];
    if (val === "A") { aces++; score += 11; }
    else if (["J", "Q", "K"].includes(val)) { score += 10; }
    else { score += parseInt(val); }
  }
  while (score > 21 && aces > 0) { score -= 10; aces--; }
  return score;
}

function evaluateHostRound(room) {
  let dScore = calculateScore(room.dealerHand);
  let dealerBJ = room.dealerHand.length === 2 && dScore === 21;
  let hostPlayer = room.players.find(p => p.socketId === room.hostSocketId);

  room.players.forEach(p => {
    if (p.socketId === room.hostSocketId) return;

    p.roundResults = p.hands.map((hand, idx) => {
      let pScore = calculateScore(hand);
      let pBJ = hand.length === 2 && pScore === 21;
      let betAmount = Number(p.bet);

      if (dealerBJ) {
        if (pBJ) {
          return 'PUSH';
        } else {
          p.balance -= betAmount;
          if (hostPlayer) hostPlayer.balance += betAmount;
          return 'LOSE';
        }
      } else if (p.statuses[idx] === 'bust' || pScore > 21) {
        p.balance -= betAmount;
        if (hostPlayer) hostPlayer.balance += betAmount;
        return 'LOSE';
      } else if (pBJ) {
        let winBonus = betAmount * 1; 
        p.balance += winBonus;
        if (hostPlayer) hostPlayer.balance -= winBonus;
        return 'BLACKJACK';
      } else if (dScore > 21 || pScore > dScore) {
        p.balance += betAmount; 
        if (hostPlayer) hostPlayer.balance -= betAmount;
        return 'WIN';
      } else if (pScore === dScore) {
        return 'PUSH'; 
      } else {
        p.balance -= betAmount;
        if (hostPlayer) hostPlayer.balance += betAmount;
        return 'LOSE';
      }
    });

    savePlayerBalanceToDB(p.username, p.balance);
  });

  if (hostPlayer) {
    savePlayerBalanceToDB(hostPlayer.username, hostPlayer.balance);
  }

  room.gameStarted = false;
  let roomId = room.roomId || Object.keys(rooms).find(k => rooms[k] === room);
  io.to(roomId).emit('round_ended', room);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`Server aktif di port ${PORT}`); });
