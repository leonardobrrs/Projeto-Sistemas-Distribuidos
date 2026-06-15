# WebSocket Chat — Demo Acadêmico
## Sistemas Distribuídos — UFAL / Instituto de Computação

---

## Tema 4: Comunicação Direta com API Gateway WebSockets

**O que demonstra:**
- Comunicação bidirecional em tempo real via WebSockets
- Sessões persistentes ao longo do tempo
- Múltiplos clientes conectados simultaneamente
- Rotas (tipos de mensagem): `chat`, `rename`, `ping/pong`, `user_joined`, `user_left`

---

## Pré-requisitos

- Node.js instalado (versão 16+)
- Terminal / CMD

Verificar instalação:
```
node -v
npm -v
```

---

## Como rodar

**1. Instalar dependências** (apenas na primeira vez):
```bash
cd websocket-demo
npm install
```

**2. Iniciar o servidor:**
```bash
npm start
```

Você verá:
```
======================================
  Servidor WebSocket rodando!
  HTTP:      http://localhost:3000
  WebSocket: ws://localhost:3000
======================================
```

**3. Abrir o cliente:**

Abra **várias abas** no navegador em:
```
http://localhost:3000
```

Cada aba representa um cliente diferente conectado ao mesmo servidor.

---

## O que testar na apresentação

| Ação | O que demonstra |
|------|----------------|
| Abrir 2+ abas | Múltiplas conexões simultâneas |
| Digitar mensagem + Enter | Comunicação bidirecional em tempo real |
| Clicar PING | Latência da conexão WebSocket |
| Renomear usuário | Propagação de evento para todos os clientes |
| Fechar uma aba | Notificação de desconexão + reconexão automática |
| Ver painel de debug | ReadyState, protocolo, URL da conexão |

---

## Estrutura do projeto

```
websocket-demo/
├── server.js      ← Servidor Node.js com ws
├── client.html    ← Interface do chat (servida pelo servidor HTTP)
├── package.json   ← Dependências
└── README.md      ← Este arquivo
```

---

## Tipos de mensagem (rotas WebSocket)

```json
// Cliente → Servidor
{ "type": "chat",   "message": "Olá!" }
{ "type": "rename", "name": "João" }
{ "type": "ping" }

// Servidor → Cliente(s)
{ "type": "welcome",     "id": 1, "name": "Usuário 1", "users": [...] }
{ "type": "chat",        "id": 1, "name": "...", "message": "...", "timestamp": "..." }
{ "type": "user_joined", "id": 2, "name": "...", "users": [...] }
{ "type": "user_left",   "id": 2, "name": "...", "users": [...] }
{ "type": "rename",      "oldName": "...", "newName": "...", "users": [...] }
{ "type": "pong",        "timestamp": 1234567890 }
```

---

## Diferença: WebSocket vs HTTP tradicional

| | HTTP | WebSocket |
|--|------|-----------|
| Conexão | Nova por requisição | Persistente |
| Direção | Cliente → Servidor | Bidirecional |
| Overhead | Headers a cada request | Apenas no handshake |
| Latência | Alta | Baixa |
| Uso ideal | REST APIs | Chat, jogos, dashboards ao vivo |

---

## Referência científica

**Real-Time Communication on AWS** — Amazon Web Services Whitepaper 2026  
https://docs.aws.amazon.com/pdfs/whitepapers/latest/real-time-communication-on-aws/real-time-communication-on-aws.pdf
