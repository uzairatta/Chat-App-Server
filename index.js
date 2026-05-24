import dotenv from "dotenv";
dotenv.config();

import express from "express";
import http from "http";
import { Server } from "socket.io";
import mongoose from "mongoose";
import Redis from "ioredis";
import cron from "node-cron";
import Room from "./models/messages.js";

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

  socket.on("join", async ({ username, room }) => {
    socket.join(room);
    console.log(`${username} joined room: ${room}`);
    const roomDoc = await Room.findOne({ roomId: room });
    if (roomDoc) {
      socket.emit("previousMessages", roomDoc.messages);
    }
  });

  socket.on("leave", ({ username, room }) => {
    socket.leave(room);
    console.log(`${username} left room: ${room}`);
  });

  socket.on("send", async (message) => {
    console.log(message);
    socket.broadcast.to(message.room).emit("message", message);
    const key = `room:${message.room}`;
    await redis.rpush(key, JSON.stringify({
      senderName: message.username,
      message: message.message,
      timestamp: new Date()
    }));
    await redis.expire(key, 5400);
  });

  socket.on("disconnect", () => {
    console.log("user disconnected", socket.id);
  });

}); // ← closing io.on("connection")

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
    await redis.del(key);
    console.log(`Synced ${parsedMessages.length} messages for room: ${roomId}`);
  }
});

server.listen(process.env.PORT || 5050, () =>
  console.log("Server running on *:5050")
);
