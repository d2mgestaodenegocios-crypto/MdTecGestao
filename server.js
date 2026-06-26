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

// ─── ARMAZENAMENTO GLOBAL ───
let sincData = {
  empresas: {},
  colunas: {},
  lembretes: {},
  notificacoes: {},
  observacoes: {},
  dados: {},
  grupos: {},
  prazos: {}
};

const DATA_FILE = './dados-sync.json';

// Carregar dados persistidos
function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const data = fs.readFileSync(DATA_FILE, 'utf8');
      sincData = JSON.parse(data);
      console.log('✅ Dados carregados do arquivo');
    }
  } catch (err) {
    console.error('⚠️ Erro ao carregar dados:', err.message);
  }
}

function saveData() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(sincData, null, 2));
  } catch (err) {
    console.error('⚠️ Erro ao salvar dados:', err.message);
  }
}

loadData();

// ─── MAPA DE USUÁRIOS CONECTADOS ───
const usuariosConectados = new Map(); // { ws → { userId, empresaIds } }

// ─── BROADCAST ───
function broadcast(mensagem, excluirWs = null) {
  const msg = JSON.stringify(mensagem);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client !== excluirWs) {
      try {
        client.send(msg);
      } catch (err) {
        console.error('Erro ao enviar:', err.message);
      }
    }
  });
}

// Broadcast apenas para usuários de uma empresa específica
function broadcastParaEmpresa(empresaId, mensagem) {
  const msg = JSON.stringify(mensagem);
  wss.clients.forEach(client => {
    const usuario = usuariosConectados.get(client);
    if (client.readyState === WebSocket.OPEN && usuario && usuario.empresaIds.includes(empresaId)) {
      try {
        client.send(msg);
      } catch (err) {
        console.error('Erro ao enviar:', err.message);
      }
    }
  });
}

// ─── API REST ───

// Obter dados sincronizados
app.get('/api/sync-data', (req, res) => {
  res.json(sincData);
});

// Atualizar qualquer dado
app.post('/api/sync-update', (req, res) => {
  const { tipo, chave, valor, empresaId } = req.body;
  
  try {
    // Validar
    if (!tipo || chave === undefined || valor === undefined) {
      return res.status(400).json({ erro: 'Campos obrigatórios faltando' });
    }

    // Inicializar tipo se não existir
    if (!sincData[tipo]) {
      sincData[tipo] = {};
    }

    // Atualizar valor
    sincData[tipo][chave] = valor;
    saveData();

    // Broadcast para todos ou para empresa específica
    const mensagem = {
      type: 'data-updated',
      tipo,
      chave,
      valor,
      empresaId,
      timestamp: new Date().toISOString()
    };

    if (empresaId) {
      broadcastParaEmpresa(empresaId, mensagem);
    } else {
      broadcast(mensagem);
    }

    res.json({ sucesso: true });
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// Obter tipo específico
app.get('/api/sync/:tipo', (req, res) => {
  const { tipo } = req.params;
  res.json(sincData[tipo] || {});
});

// ─── WEBSOCKET ───

wss.on('connection', (ws) => {
  console.log('✓ Novo cliente WebSocket conectado');
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      console.log('📨 Mensagem recebida:', msg.type);

      switch(msg.type) {
        // ─── AUTENTICAÇÃO ───
        case 'auth':
          const usuario = {
            userId: msg.userId,
            empresaIds: msg.empresaIds || [],
            timestamp: Date.now()
          };
          usuariosConectados.set(ws, usuario);
          
          // Enviar todos os dados sincronizados
          ws.send(JSON.stringify({
            type: 'sync-inicial',
            dados: sincData,
            timestamp: new Date().toISOString()
          }));

          // Notificar que usuário entrou online
          broadcast({
            type: 'usuario-online',
            userId: msg.userId,
            empresaIds: msg.empresaIds
          });

          console.log(`👤 Usuário autenticado: ${msg.userId}`);
          break;

        // ─── ATUALIZAR QUALQUER DADO ───
        case 'update':
          const { tipo, chave, valor, empresaId } = msg;
          
          if (!sincData[tipo]) {
            sincData[tipo] = {};
          }
          
          sincData[tipo][chave] = valor;
          saveData();

          // Broadcast
          const atualizacao = {
            type: 'data-updated',
            tipo,
            chave,
            valor,
            empresaId,
            userId: msg.userId,
            timestamp: new Date().toISOString()
          };

          if (empresaId) {
            broadcastParaEmpresa(empresaId, atualizacao);
          } else {
            broadcast(atualizacao);
          }

          ws.send(JSON.stringify({ type: 'ack', id: msg.id }));
          break;

        // ─── CRIAR/ADICIONAR ITEM ───
        case 'create':
          const { tipo: tipoCreate, id: itemId, valor: novoValor, empresaId: empId } = msg;
          
          if (!sincData[tipoCreate]) {
            sincData[tipoCreate] = {};
          }
          
          sincData[tipoCreate][itemId] = novoValor;
          saveData();

          const criacao = {
            type: 'item-created',
            tipo: tipoCreate,
            id: itemId,
            valor: novoValor,
            empresaId: empId,
            userId: msg.userId,
            timestamp: new Date().toISOString()
          };

          if (empId) {
            broadcastParaEmpresa(empId, criacao);
          } else {
            broadcast(criacao);
          }

          ws.send(JSON.stringify({ type: 'ack', id: msg.id }));
          break;

        // ─── DELETAR ITEM ───
        case 'delete':
          const { tipo: tipoDel, chave: chaveDel, empresaId: empIdDel } = msg;
          
          if (sincData[tipoDel] && sincData[tipoDel][chaveDel]) {
            delete sincData[tipoDel][chaveDel];
            saveData();

            const delecao = {
              type: 'item-deleted',
              tipo: tipoDel,
              chave: chaveDel,
              empresaId: empIdDel,
              userId: msg.userId,
              timestamp: new Date().toISOString()
            };

            if (empIdDel) {
              broadcastParaEmpresa(empIdDel, delecao);
            } else {
              broadcast(delecao);
            }
          }

          ws.send(JSON.stringify({ type: 'ack', id: msg.id }));
          break;

        // ─── SINCRONIZAR TUDO ───
        case 'sync-all':
          ws.send(JSON.stringify({
            type: 'sync-completo',
            dados: sincData,
            timestamp: new Date().toISOString()
          }));
          break;

        // ─── PING/PONG ───
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
      }
    } catch (err) {
      console.error('❌ Erro processando mensagem:', err.message);
      ws.send(JSON.stringify({
        type: 'erro',
        mensagem: err.message
      }));
    }
  });

  ws.on('close', () => {
    const usuario = usuariosConectados.get(ws);
    if (usuario) {
      usuariosConectados.delete(ws);
      broadcast({
        type: 'usuario-offline',
        userId: usuario.userId
      });
      console.log(`✗ Usuário desconectado: ${usuario.userId}`);
    }
  });

  ws.on('error', (err) => {
    console.error('❌ Erro WebSocket:', err.message);
  });
});

// ─── INICIAR SERVIDOR ───
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n${'='.repeat(50)}`);
  console.log(`🚀 Servidor MDTEC Sincronização`);
  console.log(`📡 HTTP: http://localhost:${PORT}`);
  console.log(`📡 WebSocket: ws://localhost:${PORT}/ws`);
  console.log(`${'='.repeat(50)}\n`);
});

// ─── GRACEFUL SHUTDOWN ───
process.on('SIGTERM', () => {
  console.log('📝 Salvando dados...');
  saveData();
  console.log('✅ Servidor encerrado');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('📝 Salvando dados...');
  saveData();
  console.log('✅ Servidor encerrado');
  process.exit(0);
});
