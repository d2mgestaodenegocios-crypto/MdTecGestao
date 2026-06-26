/**
 * 🔄 Módulo de Sincronização em Tempo Real
 * Integra avisos/anotações com WebSocket para compartilhamento entre usuários
 * 
 * Como usar:
 * 1. Instanciar: const sync = new AvisoSync(userId, empresaId, serverUrl)
 * 2. Conectar: sync.connect()
 * 3. Escutar: sync.on('aviso-atualizado', callback)
 * 4. Enviar: sync.criarAviso({ tipo, texto, data })
 */

class AvisoSync {
  constructor(userId, empresaId, serverUrl = 'ws://localhost:3000/ws') {
    this.userId = userId;
    this.empresaId = empresaId;
    this.serverUrl = serverUrl;
    this.ws = null;
    this.listeners = new Map();
    this.isConnected = false;
    this.reconectarTentativas = 0;
    this.maxReconectarTentativas = 5;
  }

  // ─── CONEXÃO ───
  connect() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.serverUrl);
        
        this.ws.onopen = () => {
          console.log('✓ Conectado ao servidor de avisos');
          this.isConnected = true;
          this.reconectarTentativas = 0;
          
          // Se inscrever para atualizações dessa empresa
          this.enviar({
            type: 'subscribe',
            userId: this.userId,
            empresaId: this.empresaId
          });
          
          this.emit('conectado');
          resolve();
        };
        
        this.ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            this.processarMensagem(msg);
          } catch (err) {
            console.error('Erro processando mensagem:', err);
          }
        };
        
        this.ws.onerror = (err) => {
          console.error('❌ Erro WebSocket:', err);
          this.emit('erro', err);
          reject(err);
        };
        
        this.ws.onclose = () => {
          console.log('✗ Desconectado do servidor');
          this.isConnected = false;
          this.emit('desconectado');
          
          // Tentar reconectar
          if (this.reconectarTentativas < this.maxReconectarTentativas) {
            this.reconectarTentativas++;
            const delay = Math.min(1000 * this.reconectarTentativas, 10000);
            console.log(`⟳ Reconectando em ${delay}ms...`);
            setTimeout(() => this.connect(), delay);
          }
        };
      } catch (err) {
        reject(err);
      }
    });
  }

  // ─── ENVIO DE MENSAGENS ───
  enviar(msg) {
    if (this.ws && this.isConnected) {
      this.ws.send(JSON.stringify(msg));
    } else {
      console.warn('⚠ WebSocket não está conectado');
    }
  }

  // ─── AÇÕES (CRUD) ───
  criarAviso(aviso) {
    this.enviar({
      type: 'criar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      aviso: {
        tipo: aviso.tipo || 'anotacao',
        texto: aviso.texto || '',
        data: aviso.data || new Date().toLocaleDateString('pt-BR'),
        feito: aviso.feito || false
      }
    });
  }

  atualizarAviso(avisoId, updates) {
    this.enviar({
      type: 'atualizar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      avisoId,
      updates
    });
  }

  deletarAviso(avisoId) {
    this.enviar({
      type: 'deletar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      avisoId
    });
  }

  // ─── PROCESSAMENTO DE MENSAGENS ───
  processarMensagem(msg) {
    switch(msg.type) {
      case 'avisos-carregados':
        this.emit('avisos-carregados', msg.avisos);
        break;
      case 'aviso-criado':
        if (msg.empresaId === this.empresaId) {
          this.emit('aviso-criado', msg.aviso);
        }
        break;
      case 'aviso-atualizado':
        if (msg.empresaId === this.empresaId) {
          this.emit('aviso-atualizado', msg.avisoId, msg.updates);
        }
        break;
      case 'aviso-deletado':
        if (msg.empresaId === this.empresaId) {
          this.emit('aviso-deletado', msg.avisoId);
        }
        break;
      case 'usuario-online':
        this.emit('usuario-online', msg.userId);
        break;
      case 'usuario-offline':
        this.emit('usuario-offline', msg.userId);
        break;
    }
  }

  // ─── EVENT EMITTER ───
  on(evento, callback) {
    if (!this.listeners.has(evento)) {
      this.listeners.set(evento, []);
    }
    this.listeners.get(evento).push(callback);
  }

  off(evento, callback) {
    if (this.listeners.has(evento)) {
      const callbacks = this.listeners.get(evento);
      const idx = callbacks.indexOf(callback);
      if (idx > -1) {
        callbacks.splice(idx, 1);
      }
    }
  }

  emit(evento, ...args) {
    if (this.listeners.has(evento)) {
      this.listeners.get(evento).forEach(cb => {
        try {
          cb(...args);
        } catch (err) {
          console.error(`Erro em listener ${evento}:`, err);
        }
      });
    }
  }

  // ─── DESCONECTAR ───
  disconnect() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
      this.isConnected = false;
    }
  }
}

// Exportar para uso global
if (typeof window !== 'undefined') {
  window.AvisoSync = AvisoSync;
}
