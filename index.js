import express from "express";
import http from "http";
import { Server } from "socket.io";

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  }
});

io.on("connection", (socket) => {
  console.log("a user connected", socket.id);

  // Join a room -- Event Listen
  socket.on("join", (roomId) => {
    socket.join(roomId);
  });

  socket.on("leave", (roomId) => {
    socket.leave(roomId);
  });

  // Broadcast to room
  socket.on("send", (message) => {
    console.log(message);
    socket.to(message.room).emit("message", message); // Event Fire
  });

}); 

server.listen(5050, () => 
  console.log("Server running on *:5050")
);