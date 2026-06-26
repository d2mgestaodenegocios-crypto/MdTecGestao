const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ─── ARMAZENAMENTO EM MEMÓRIA (substitua por banco de dados) ───
let avisos = {};
let usuarios = new Map(); // conectados: { ws, userId, empresaId }

// Carregar avisos do arquivo JSON se existir
const AVISOS_FILE = './avisos.json';
function loadAvisos() {
  try {
    if (fs.existsSync(AVISOS_FILE)) {
      const data = fs.readFileSync(AVISOS_FILE, 'utf8');
      avisos = JSON.parse(data);
    }
  } catch (err) {
    console.error('Erro ao carregar avisos:', err);
  }
}

function saveAvisos() {
  try {
    fs.writeFileSync(AVISOS_FILE, JSON.stringify(avisos, null, 2));
  } catch (err) {
    console.error('Erro ao salvar avisos:', err);
  }
}

loadAvisos();

// ─── API REST ───
app.get('/api/avisos/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const empresa = avisos[empresaId] || [];
  res.json(empresa);
});

app.post('/api/avisos/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const aviso = req.body;
  
  if (!avisos[empresaId]) {
    avisos[empresaId] = [];
  }
  
  avisos[empresaId].push(aviso);
  saveAvisos();
  
  // Notificar todos os usuários dessa empresa via WebSocket
  broadcast({
    type: 'aviso-criado',
    empresaId,
    aviso
  });
  
  res.json({ success: true, aviso });
});

app.put('/api/avisos/:empresaId/:avisoId', (req, res) => {
  const { empresaId, avisoId } = req.params;
  const updates = req.body;
  
  if (!avisos[empresaId]) {
    return res.status(404).json({ error: 'Empresa não encontrada' });
  }
  
  const aviso = avisos[empresaId].find(a => a.id === avisoId);
  if (!aviso) {
    return res.status(404).json({ error: 'Aviso não encontrado' });
  }
  
  Object.assign(aviso, updates);
  saveAvisos();
  
  // Notificar todos os usuários
  broadcast({
    type: 'aviso-atualizado',
    empresaId,
    avisoId,
    updates
  });
  
  res.json({ success: true, aviso });
});

app.delete('/api/avisos/:empresaId/:avisoId', (req, res) => {
  const { empresaId, avisoId } = req.params;
  
  if (!avisos[empresaId]) {
    return res.status(404).json({ error: 'Empresa não encontrada' });
  }
  
  avisos[empresaId] = avisos[empresaId].filter(a => a.id !== avisoId);
  saveAvisos();
  
  broadcast({
    type: 'aviso-deletado',
    empresaId,
    avisoId
  });
  
  res.json({ success: true });
});

// ─── WEBSOCKET ───
function broadcast(message) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  });
}

wss.on('connection', (ws) => {
  console.log('✓ Cliente WebSocket conectado');
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      switch(msg.type) {
        case 'subscribe':
          // Cliente se inscreve para atualizações de uma empresa
          ws.userId = msg.userId;
          ws.empresaId = msg.empresaId;
          usuarios.set(ws, { userId: msg.userId, empresaId: msg.empresaId });
          
          // Enviar todos os avisos atuais dessa empresa
          const empresa = avisos[msg.empresaId] || [];
          ws.send(JSON.stringify({
            type: 'avisos-carregados',
            empresaId: msg.empresaId,
            avisos: empresa
          }));
          
          // Notificar que usuário entrou
          broadcast({
            type: 'usuario-online',
            userId: msg.userId,
            empresaId: msg.empresaId
          });
          break;
          
        case 'criar-aviso':
          const novoAviso = {
            id: 'aviso_' + Date.now(),
            ...msg.aviso,
            criadoPor: msg.userId,
            criadoEm: new Date().toISOString()
          };
          
          if (!avisos[msg.empresaId]) {
            avisos[msg.empresaId] = [];
          }
          avisos[msg.empresaId].push(novoAviso);
          saveAvisos();
          
          broadcast({
            type: 'aviso-criado',
            empresaId: msg.empresaId,
            aviso: novoAviso
          });
          break;
          
        case 'atualizar-aviso':
          if (avisos[msg.empresaId]) {
            const aviso = avisos[msg.empresaId].find(a => a.id === msg.avisoId);
            if (aviso) {
              Object.assign(aviso, msg.updates);
              aviso.atualizadoEm = new Date().toISOString();
              aviso.atualizadoPor = msg.userId;
              saveAvisos();
              
              broadcast({
                type: 'aviso-atualizado',
                empresaId: msg.empresaId,
                avisoId: msg.avisoId,
                updates: msg.updates
              });
            }
          }
          break;
          
        case 'deletar-aviso':
          if (avisos[msg.empresaId]) {
            avisos[msg.empresaId] = avisos[msg.empresaId].filter(a => a.id !== msg.avisoId);
            saveAvisos();
            
            broadcast({
              type: 'aviso-deletado',
              empresaId: msg.empresaId,
              avisoId: msg.avisoId
            });
          }
          break;
      }
    } catch (err) {
      console.error('Erro processando mensagem WebSocket:', err);
    }
  });
  
  ws.on('close', () => {
    const user = usuarios.get(ws);
    usuarios.delete(ws);
    console.log('✗ Cliente desconectado');
    
    if (user) {
      broadcast({
        type: 'usuario-offline',
        userId: user.userId
      });
    }
  });
  
  ws.on('error', (err) => {
    console.error('Erro WebSocket:', err);
  });
});

// ─── INICIAR SERVIDOR ───
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Servidor rodando em http://localhost:${PORT}`);
  console.log(`📡 WebSocket disponível em ws://localhost:${PORT}/ws`);
});
