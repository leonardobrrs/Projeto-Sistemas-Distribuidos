# Projeto de Sistemas Distribuídos: WebSocket Clusterizado com Redis

Este projeto é uma demonstração prática de comunicação bidirecional em tempo real utilizando WebSockets. O sistema foi concebido com uma arquitetura distribuída escalável, simulando múltiplos nós de servidor interligados através de um Message Broker, aplicando conceitos avançados de tolerância a falhas e sincronização de estado.

## Arquitetura e Conceitos Implementados

Este projeto permite executar múltiplos nós independentes localmente (em portas diferentes), que comunicam entre si para formar um Cluster.

* **Message Broker (Redis Pub/Sub):** Os nós Node.js não partilham memória RAM. Para distribuir as mensagens entre os utilizadores ligados em portas diferentes, utilizamos o padrão *Publish/Subscribe* do Redis.
* **Sincronização de Estado em Memória:** A lista global de utilizadores online é construída em tempo real através da troca de mensagens de sincronização (`sync_request` e `sync_response`) no momento em que um nó é iniciado, eliminando a necessidade de uma base de dados central.
* **Tolerância a Falhas (Heartbeat):** Cada nó emite um sinal de vida ("Estou vivo!") a cada 5 segundos. Se um nó "morrer" subitamente (falha de hardware/rede) e deixar de emitir sinais por 15 segundos, o cluster deteta a falha (Timeout) e remove os utilizadores desse nó (evitando *Stale Data*).
* **Desligamento Gracioso (Graceful Shutdown):** Interceção do sinal de interrupção do sistema (`Ctrl + C`) para avisar o cluster de que o nó está a encerrar de forma planeada, permitindo a saída limpa dos utilizadores.
* **Prevenção de Concorrência:** O sistema garante a regra de "1 Porta = 1 Nó = 1 Utilizador", bloqueando abas extras e mantendo uma identidade persistente para o nó em caso de reconexão.

## Pré-requisitos

Para executar este projeto, necessita de ter instalado na sua máquina:
* **Node.js** (v14 ou superior)
* **Servidor Redis** (A correr localmente via WSL, Docker, ou ficheiro nativo Windows, na porta padrão `6379`).

## Como Instalar e Executar

1. Clone o repositório e instale as dependências:
   npm install

2. Certifique-se de que o Redis está a correr em segundo plano:
   redis-server

3. Inicie dois ou mais nós em terminais separados para simular o ambiente distribuído:
   * Terminal 1: node server.js 3000
   * Terminal 2: node server.js 3001
   * Terminal 3: node server.js 3002

4. Abra o navegador e aceda a cada nó:
   * http://localhost:3000
   * http://localhost:3001

## Como testar a Tolerância a Falhas (Chaos Testing)

Para comprovar a robustez do sistema, realize os seguintes testes:

1. **Teste de Desligamento Planeado:** No terminal da porta 3001, pressione `Ctrl + C`. Observe como o utilizador 3001 desaparece *instantaneamente* da lista da porta 3000, e uma mensagem de notificação é exibida.
2. **Teste de Queda Abrupta (Heartbeat):** Inicie novamente o nó 3001. Desta vez, feche a janela do terminal abruptamente no "X" (ou termine o processo no Gestor de Tarefas). Na aba da porta 3000, o utilizador continuará visível. Aguarde cerca de 15 segundos sem mexer em nada e o sistema detetará o *timeout*, removendo o utilizador caído automaticamente.

## Referência Científica e Contexto

A arquitetura final deste projeto simula localmente a mesma filosofia de desacoplamento proposta em arquiteturas Cloud Serverless modernas.

* **Real-Time Communication on AWS**
* *Amazon Web Services - AWS Whitepaper 2026*
* Link para o PDF: https://docs.aws.amazon.com/pdfs/whitepapers/latest/real-time-communication-on-aws/real-time-communication-on-aws.pdf

*(Nota Académica: Em ambiente de produção AWS, o papel de manter as conexões abertas seria do AWS API Gateway, e o roteamento de mensagens / estado seria gerido por instâncias temporárias de AWS Lambda apoiadas por um banco de dados rápido como o DynamoDB. Este projeto emula esse comportamento trocando os componentes geridos pela AWS pelo Node.js e Redis locais).*