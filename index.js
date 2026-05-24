import dotenv from "dotenv";
dotenv.config();

import express from "express";
import http from "http";
import { Server } from "socket.io";
import mongoose from "mongoose";
import Redis from "ioredis";
import cron from "node-cron";
import Room from "./models/messages.js";

console.log("MONGODB_URI:", process.env.MONGODB_URI);
console.log("REDIS_URL:", process.env.REDIS_URL);

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  }
});

mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log("MongoDB connected"))
  .catch((err) => console.log(err));

const redis = new Redis(process.env.REDIS_URL);

io.on("connection", (socket) => {
  console.log("a user connected", socket.id);

  socket.on("join", async (roomId) => {
    socket.join(roomId);
    const room = await Room.findOne({ roomId });
    if (room) {
      socket.emit("previousMessages", room.messages);
    }
  });

  socket.on("leave", (roomId) => {
    socket.leave(roomId);
  });

  socket.on("send", async (message) => {
    console.log(message);
    io.to(message.room).emit("message", message);
    const key = `room:${message.room}`;
    await redis.rpush(key, JSON.stringify({
      senderName: message.username,
      message: message.message,
      timestamp: new Date()
    }));
    await redis.expire(key, 5400);
  });
});

cron.schedule("* * * * *", async () => {
  console.log("Running cron job - syncing Redis to MongoDB");
  const keys = await redis.keys("room:*");
  for (const key of keys) {
    const roomId = key.split(":")[1];
    const messages = await redis.lrange(key, 0, -1);
    if (messages.length === 0) continue;
    const parsedMessages = messages.map(m => JSON.parse(m));
    await Room.findOneAndUpdate(
      { roomId },
      { $push: { messages: { $each: parsedMessages } } },
      { upsert: true, new: true }
    );
    console.log(`Synced ${parsedMessages.length} messages for room: ${roomId}`);
  }
});

server.listen(process.env.PORT || 5050, () =>
  console.log("Server running on *:5050")
);