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
let avisos = {}; // {empresaId: [{id, tipo, texto, data, feito, visualizado, criadoPor, criadoEm}]}
let colunas = {}; // {empresaId: [{key, icon, label, color}]}
let usuarios = new Map(); // {ws: {userId, empresaId, role}}

// Carregar dados persistidos
const AVISOS_FILE = './avisos.json';
const COLUNAS_FILE = './colunas.json';

function loadAvisos() {
  try {
    if (fs.existsSync(AVISOS_FILE)) {
      const data = fs.readFileSync(AVISOS_FILE, 'utf8');
      avisos = JSON.parse(data);
    }
  } catch (err) {
    console.error('❌ Erro ao carregar avisos:', err);
  }
}

function saveAvisos() {
  try {
    fs.writeFileSync(AVISOS_FILE, JSON.stringify(avisos, null, 2));
  } catch (err) {
    console.error('❌ Erro ao salvar avisos:', err);
  }
}

function loadColunas() {
  try {
    if (fs.existsSync(COLUNAS_FILE)) {
      const data = fs.readFileSync(COLUNAS_FILE, 'utf8');
      colunas = JSON.parse(data);
    }
  } catch (err) {
    console.error('❌ Erro ao carregar colunas:', err);
  }
}

function saveColunas() {
  try {
    fs.writeFileSync(COLUNAS_FILE, JSON.stringify(colunas, null, 2));
  } catch (err) {
    console.error('❌ Erro ao salvar colunas:', err);
  }
}

loadAvisos();
loadColunas();

// ─── HELPER: Broadcast para uma empresa ───
function broadcastToEmpresa(empresaId, message) {
  wss.clients.forEach(client => {
    const user = usuarios.get(client);
    if (client.readyState === WebSocket.OPEN && user && user.empresaId === empresaId) {
      client.send(JSON.stringify(message));
    }
  });
}

// ─── HELPER: Broadcast para todos (debug) ───
function broadcast(message) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  });
}

// ─── API REST (BACKUP para operações HTTP) ───

app.get('/api/avisos/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const empresa = avisos[empresaId] || [];
  // Filtrar avisos resolvidos (feito=true ou visualizado=true) para novos usuários
  const ativos = empresa.filter(a => !a.feito && !a.visualizado);
  res.json(ativos);
});

app.get('/api/colunas/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const cols = colunas[empresaId] || [];
  res.json(cols);
});

app.post('/api/avisos/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const aviso = req.body;
  
  if (!avisos[empresaId]) {
    avisos[empresaId] = [];
  }
  
  const novoAviso = {
    id: aviso.id || 'aviso_' + Date.now(),
    tipo: aviso.tipo || 'anotacao',
    texto: aviso.texto || '',
    data: aviso.data || new Date().toLocaleDateString('pt-BR'),
    feito: aviso.feito || false,
    visualizado: aviso.visualizado || false,
    criadoEm: new Date().toISOString()
  };
  
  avisos[empresaId].push(novoAviso);
  saveAvisos();
  
  broadcastToEmpresa(empresaId, {
    type: 'aviso-criado',
    empresaId,
    aviso: novoAviso
  });
  
  res.json({ success: true, aviso: novoAviso });
});

app.put('/api/colunas/:empresaId', (req, res) => {
  const { empresaId } = req.params;
  const role = req.headers['x-user-role'] || 'user';
  
  // Apenas admin pode alterar colunas
  if (role !== 'admin') {
    return res.status(403).json({ error: 'Apenas admins podem alterar colunas' });
  }
  
  const cols = req.body;
  colunas[empresaId] = cols;
  saveColunas();
  
  broadcastToEmpresa(empresaId, {
    type: 'colunas-atualizadas',
    empresaId,
    colunas: cols,
    atualizadoEm: new Date().toISOString()
  });
  
  res.json({ success: true, colunas: cols });
});

// ─── WEBSOCKET ───

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
          ws.role = msg.role || 'user';
          usuarios.set(ws, { userId: msg.userId, empresaId: msg.empresaId, role: ws.role });
          
          console.log(`👤 ${msg.userId} (${ws.role}) entrou em ${msg.empresaId}`);
          
          // Enviar todos os avisos ATIVOS dessa empresa (excluir feito/visualizado)
          const empresa = avisos[msg.empresaId] || [];
          const avisosAtivos = empresa.filter(a => !a.feito && !a.visualizado);
          
          ws.send(JSON.stringify({
            type: 'avisos-carregados',
            empresaId: msg.empresaId,
            avisos: avisosAtivos
          }));
          
          // Se tem colunas customizadas, enviar também
          if (colunas[msg.empresaId]) {
            ws.send(JSON.stringify({
              type: 'colunas-carregadas',
              empresaId: msg.empresaId,
              colunas: colunas[msg.empresaId]
            }));
          }
          
          // Notificar que usuário entrou
          broadcastToEmpresa(msg.empresaId, {
            type: 'usuario-online',
            userId: msg.userId,
            role: ws.role,
            empresaId: msg.empresaId
          });
          break;
          
        case 'criar-aviso':
          const novoAviso = {
            id: msg.aviso.id || 'aviso_' + Date.now(),
            tipo: msg.aviso.tipo || 'anotacao',
            texto: msg.aviso.texto || '',
            data: msg.aviso.data || new Date().toLocaleDateString('pt-BR'),
            feito: msg.aviso.feito || false,
            visualizado: msg.aviso.visualizado || false,
            criadoPor: msg.userId,
            criadoEm: msg.aviso.criadoEm || new Date().toISOString()
          };
          
          if (!avisos[msg.empresaId]) {
            avisos[msg.empresaId] = [];
          }
          
          // Evitar duplicatas: verificar se já existe
          const existe = avisos[msg.empresaId].find(a => a.id === novoAviso.id);
          if (!existe) {
            avisos[msg.empresaId].push(novoAviso);
            saveAvisos();
            
            broadcastToEmpresa(msg.empresaId, {
              type: 'aviso-criado',
              empresaId: msg.empresaId,
              aviso: novoAviso
            });
          }
          break;
          
        case 'atualizar-aviso':
          if (avisos[msg.empresaId]) {
            const aviso = avisos[msg.empresaId].find(a => a.id === msg.avisoId);
            if (aviso) {
              Object.assign(aviso, msg.updates);
              aviso.atualizadoPor = msg.userId;
              saveAvisos();
              
              broadcastToEmpresa(msg.empresaId, {
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
            
            broadcastToEmpresa(msg.empresaId, {
              type: 'aviso-deletado',
              empresaId: msg.empresaId,
              avisoId: msg.avisoId
            });
          }
          break;

        case 'atualizar-colunas':
          // Apenas admins podem alterar colunas
          if (ws.role !== 'admin') {
            ws.send(JSON.stringify({
              type: 'permissao-negada',
              mensagem: '❌ Apenas admins podem modificar colunas'
            }));
            break;
          }

          colunas[msg.empresaId] = msg.colunas;
          saveColunas();

          console.log(`⚙️ Colunas atualizadas em ${msg.empresaId} por ${msg.userId}`);

          broadcastToEmpresa(msg.empresaId, {
            type: 'colunas-atualizadas',
            empresaId: msg.empresaId,
            colunas: msg.colunas,
            atualizadoPor: msg.userId,
            atualizadoEm: msg.atualizadoEm || new Date().toISOString()
          });
          break;
      }
    } catch (err) {
      console.error('❌ Erro processando mensagem WebSocket:', err);
    }
  });
  
  ws.on('close', () => {
    const user = usuarios.get(ws);
    usuarios.delete(ws);
    console.log('✗ Cliente desconectado');
    
    if (user) {
      broadcastToEmpresa(user.empresaId, {
        type: 'usuario-offline',
        userId: user.userId,
        empresaId: user.empresaId
      });
    }
  });
  
  ws.on('error', (err) => {
    console.error('❌ Erro WebSocket:', err);
  });
});

// ─── INICIAR SERVIDOR ───
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Servidor rodando em http://localhost:${PORT}`);
  console.log(`📡 WebSocket disponível em ws://localhost:${PORT}/ws`);
  console.log(`💾 Avisos persistidos em ${AVISOS_FILE}`);
  console.log(`⚙️  Colunas persistidas em ${COLUNAS_FILE}`);
});
