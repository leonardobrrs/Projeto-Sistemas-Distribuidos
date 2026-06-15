const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { createClient } = require('redis'); 

const PORT = process.argv[2] ? parseInt(process.argv[2]) : 3000;

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
let currentNodeName = `Usuário ${PORT}`;
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
    if (nodeId !== PORT) {
      allUsers = allUsers.concat(users);
    }
  });
  return allUsers;
}

const pub = createClient();
const sub = createClient();

pub.on('error', (err) => console.error('Erro no Redis Pub:', err));
sub.on('error', (err) => console.error('Erro no Redis Sub:', err));

Promise.all([pub.connect(), sub.connect()]).then(() => {
  console.log('🔗 Conectado ao Message Broker (Redis)');
  
  pub.publish('chat_global', JSON.stringify({
    type: 'sync_request',
    nodeId: PORT
  }));
  
  sub.subscribe('chat_global', (message) => {
    const data = JSON.parse(message);
    
    if (data.type === 'sync_request' && data.nodeId !== PORT) {
      pub.publish('chat_global', JSON.stringify({
        type: 'sync_response',
        nodeId: PORT,
        localUsers: getConnectedUsers()
      }));
      return;
    }

    if (data.type === 'sync_response' && data.nodeId !== PORT) {
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
      
      let disconnectedUserName = `Usuário ${data.nodeId}`;
      const usersOnNode = globalUsersByNode.get(data.nodeId);
      if (usersOnNode && usersOnNode.length > 0) {
        disconnectedUserName = usersOnNode[0].name;
      }

      globalUsersByNode.delete(data.nodeId);
      nodeLastSeen.delete(data.nodeId);
      
      broadcastAll({ 
        type: 'user_left', 
        message: `${disconnectedUserName} foi desconectado.`,
        users: getGlobalUsers() 
      });
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

    if (excludeNode === PORT && excludeId) {
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
  if (clients.size >= 1) {
    console.log(`[BLOQUEIO] Aba extra rejeitada. A porta ${PORT} já está em uso.`);
    ws.close();
    return;
  }

  const clientId = PORT;
  const clientName = currentNodeName;

  clients.set(ws, { id: clientId, name: clientName });
  console.log(`[CONEXÃO] ${clientName} conectado.`);

  ws.send(JSON.stringify({
    type: 'welcome',
    id: clientId,
    name: clientName,
    message: `Bem-vindo, ${clientName}! Você está conectado ao servidor WebSocket.`,
    users: getGlobalUsers()
  }));

  pub.publish('chat_global', JSON.stringify({
    type: 'user_joined',
    nodeId: PORT,
    localUsers: getConnectedUsers(),
    excludeNode: PORT,     
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
        nodeId: PORT,
        localUsers: getConnectedUsers(),
        id: senderInfo.id,
        name: senderInfo.name,
        message: data.message,
        timestamp: new Date().toLocaleTimeString('pt-BR')
      }));

    } else if (data.type === 'rename') {
      const oldName = senderInfo.name;
      
      currentNodeName = data.name || senderInfo.name;
      senderInfo.name = currentNodeName;
      clients.set(ws, senderInfo);
      
      pub.publish('chat_global', JSON.stringify({
        type: 'rename',
        nodeId: PORT,
        localUsers: getConnectedUsers(),
        id: senderInfo.id,
        oldName,
        newName: currentNodeName,
        message: `${oldName} agora se chama ${currentNodeName}.`
      }));

    } else if (data.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (!info) return;
    
    clients.delete(ws);
    console.log(`[DESCONEXÃO] ${info.name}. A porta ${PORT} está livre novamente.`);
    
    pub.publish('chat_global', JSON.stringify({
      type: 'user_left',
      nodeId: PORT,
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
    nodeId: PORT
  }));
}, 5000);

setInterval(() => {
  const now = Date.now();
  nodeLastSeen.forEach((lastTime, nodeId) => {
    if (now - lastTime > 15000) {
      console.log(`[FALHA DETECTADA] Nó ${nodeId} não responde. Removendo seus utilizadores...`);
      
      let disconnectedUserName = `Usuário ${nodeId}`;
      const usersOnNode = globalUsersByNode.get(nodeId);
      if (usersOnNode && usersOnNode.length > 0) {
        disconnectedUserName = usersOnNode[0].name;
      }

      globalUsersByNode.delete(nodeId);
      nodeLastSeen.delete(nodeId);
      
      broadcastAll({
        type: 'user_left', 
        message: `${disconnectedUserName} foi desconectado.`,
        users: getGlobalUsers()
      });
    }
  });
}, 5000);

process.on('SIGINT', async () => {
  console.log(`\n[SISTEMA] Encerrando o nó ${PORT}...`);
  try {
    await pub.publish('chat_global', JSON.stringify({
      type: 'node_dead',
      nodeId: PORT
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
  console.log(`  HTTP:      http://localhost:${PORT}`);
  console.log(`  WebSocket: ws://localhost:${PORT}`);
  console.log(`======================================\n`);
});