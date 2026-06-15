const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { createClient } = require('redis'); 

const PORT = process.env.PORT || (process.argv[2] ? parseInt(process.argv[2]) : 3000);
const NODE_ID = process.env.NODE_ID || Math.floor(1000 + Math.random() * 9000).toString();

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    const filePath = path.join(__dirname, 'client.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end('Not found');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocket.Server({ server });

const clients = new Map();
let clientCounter = 0;
const globalUsersByNode = new Map();
const nodeLastSeen = new Map(); 

function getConnectedUsers() {
  const users = [];
  clients.forEach((info) => {
    users.push({ id: info.id, name: info.name });
  });
  return users;
}

function getGlobalUsers() {
  let allUsers = getConnectedUsers();
  globalUsersByNode.forEach((users, nodeId) => {
    if (nodeId !== NODE_ID) {
      allUsers = allUsers.concat(users);
    }
  });
  return allUsers;
}

const pub = createClient({ url: process.env.REDIS_URL });
const sub = createClient({ url: process.env.REDIS_URL });

pub.on('error', (err) => console.error('Erro no Redis Pub:', err));
sub.on('error', (err) => console.error('Erro no Redis Sub:', err));

Promise.all([pub.connect(), sub.connect()]).then(() => {
  console.log(`🔗 Conectado ao Message Broker (Redis) - Identidade do Nó: ${NODE_ID}`);
  
  pub.publish('chat_global', JSON.stringify({
    type: 'sync_request',
    nodeId: NODE_ID
  }));
  
  sub.subscribe('chat_global', (message) => {
    const data = JSON.parse(message);
    
    if (data.type === 'sync_request' && data.nodeId !== NODE_ID) {
      pub.publish('chat_global', JSON.stringify({
        type: 'sync_response',
        nodeId: NODE_ID,
        localUsers: getConnectedUsers()
      }));
      return;
    }

    if (data.type === 'sync_response' && data.nodeId !== NODE_ID) {
      globalUsersByNode.set(data.nodeId, data.localUsers);
      nodeLastSeen.set(data.nodeId, Date.now());
      broadcastAll({ type: 'update_user_list', users: getGlobalUsers() });
      return;
    }

    if (data.type === 'heartbeat') {
      nodeLastSeen.set(data.nodeId, Date.now());
      return; 
    }

    if (data.type === 'node_dead') {
      console.log(`[AVISO] O nó ${data.nodeId} foi desligado corretamente.`);
      
      const usersOnNode = globalUsersByNode.get(data.nodeId) || [];
      const dropCount = usersOnNode.length;

      globalUsersByNode.delete(data.nodeId);
      nodeLastSeen.delete(data.nodeId);
      
      if (dropCount > 0) {
        broadcastAll({ 
          type: 'user_left', 
          message: `Servidor encerrado. ${dropCount} usuário(s) desconectado(s).`, 
          users: getGlobalUsers() 
        });
      }
      return;
    }
    
    if (data.nodeId && data.localUsers) {
      globalUsersByNode.set(data.nodeId, data.localUsers);
    }
    
    data.users = getGlobalUsers();
    
    const excludeNode = data.excludeNode;
    const excludeId = data.excludeId;
    delete data.localUsers;
    delete data.nodeId;
    delete data.excludeNode;
    delete data.excludeId;

    if (excludeNode === NODE_ID && excludeId) {
      broadcast(data, excludeId);
    } else {
      broadcastAll(data);
    }
  });
});

function broadcast(data, excludeId = null) {
  const message = JSON.stringify(data);
  clients.forEach((info, ws) => {
    if (info.id !== excludeId && ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

function broadcastAll(data) {
  const message = JSON.stringify(data);
  clients.forEach((info, ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

wss.on('connection', (ws) => {
  clientCounter++;
  
  const clientId = `${NODE_ID}-${clientCounter}`;
  const clientName = `Usuário ${clientId}`;

  clients.set(ws, { id: clientId, name: clientName });
  console.log(`[CONEXÃO] ${clientName} conectado. Total local: ${clients.size}`);

  ws.send(JSON.stringify({
    type: 'welcome',
    id: clientId,
    name: clientName,
    message: `Bem-vindo, ${clientName}! Você está conectado ao servidor WebSocket.`,
    users: getGlobalUsers()
  }));

  pub.publish('chat_global', JSON.stringify({
    type: 'user_joined',
    nodeId: NODE_ID,
    localUsers: getConnectedUsers(),
    excludeNode: NODE_ID,     
    excludeId: clientId, 
    id: clientId,
    name: clientName,
    message: `${clientName} entrou na sala.`
  }));

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }

    const senderInfo = clients.get(ws);

    if (data.type === 'chat') {
      pub.publish('chat_global', JSON.stringify({
        type: 'chat',
        nodeId: NODE_ID,
        localUsers: getConnectedUsers(),
        id: senderInfo.id,
        name: senderInfo.name,
        message: data.message,
        timestamp: new Date().toLocaleTimeString('pt-BR')
      }));

    } else if (data.type === 'rename') {
      const oldName = senderInfo.name;
      
      senderInfo.name = data.name || senderInfo.name;
      clients.set(ws, senderInfo);
      
      pub.publish('chat_global', JSON.stringify({
        type: 'rename',
        nodeId: NODE_ID,
        localUsers: getConnectedUsers(),
        id: senderInfo.id,
        oldName,
        newName: senderInfo.name,
        message: `${oldName} agora se chama ${senderInfo.name}.`
      }));

    } else if (data.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (!info) return;
    
    clients.delete(ws);
    console.log(`[DESCONEXÃO] ${info.name}. Total local: ${clients.size}`);
    
    pub.publish('chat_global', JSON.stringify({
      type: 'user_left',
      nodeId: NODE_ID,
      localUsers: getConnectedUsers(), 
      id: info.id,
      name: info.name,
      message: `${info.name} saiu da sala.`
    }));
  });

  ws.on('error', (err) => {
    console.error(`[ERRO] ${err.message}`);
  });
});

setInterval(() => {
  pub.publish('chat_global', JSON.stringify({
    type: 'heartbeat',
    nodeId: NODE_ID
  }));
}, 5000);

setInterval(() => {
  const now = Date.now();
  nodeLastSeen.forEach((lastTime, nodeId) => {
    if (now - lastTime > 15000) {
      console.log(`[FALHA DETECTADA] Nó ${nodeId} não responde. Removendo seus utilizadores...`);
      
      const usersOnNode = globalUsersByNode.get(nodeId) || [];
      const dropCount = usersOnNode.length;

      globalUsersByNode.delete(nodeId);
      nodeLastSeen.delete(nodeId);
      
      if (dropCount > 0) {
        broadcastAll({
          type: 'user_left', 
          message: `O servidor ${nodeId} caiu (Timeout). ${dropCount} usuário(s) desconectado(s).`, 
          users: getGlobalUsers()
        });
      }
    }
  });
}, 5000);

process.on('SIGINT', async () => {
  console.log(`\n[SISTEMA] Encerrando o nó ${NODE_ID} graciosamente...`);
  try {
    await pub.publish('chat_global', JSON.stringify({
      type: 'node_dead',
      nodeId: NODE_ID
    }));
    setTimeout(() => {
      process.exit(0);
    }, 300);
  } catch (err) {
    process.exit(0);
  }
});

server.listen(PORT, () => {
  console.log(`\n======================================`);
  console.log(`  Servidor WebSocket rodando!`);
  console.log(`  Porta Física: ${PORT}`);
  console.log(`  Identidade (Nó): ${NODE_ID}`);
  console.log(`======================================\n`);
});