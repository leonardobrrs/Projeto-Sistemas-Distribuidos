@echo off
echo Iniciando os nos do cluster local...

start "Node 3000" cmd /k "node server.js 3000"

start "Node 3001" cmd /k "node server.js 3001"

echo Servidores iniciados com sucesso!