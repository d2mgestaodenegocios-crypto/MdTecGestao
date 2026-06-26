/**
 * 🔄 Módulo de Sincronização em Tempo Real v2.0
 * Integra avisos/anotações e COLUNAS com WebSocket
 * 
 * Como usar:
 * 1. Instanciar: const sync = new AvisoSync(userId, empresaId, role, serverUrl)
 * 2. Conectar: sync.connect()
 * 3. Escutar: sync.on('evento', callback)
 * 4. Enviar: sync.criarAviso({ ... }), sync.atualizarColunas({ ... })
 */

class AvisoSync {
  constructor(userId, empresaId, role = 'user', serverUrl = 'ws://localhost:3000/ws') {
    this.userId = userId;
    this.empresaId = empresaId;
    this.role = role; // 'admin' ou 'user'
    this.serverUrl = serverUrl;
    this.ws = null;
    this.listeners = new Map();
    this.isConnected = false;
    this.reconectarTentativas = 0;
    this.maxReconectarTentativas = 5;
    this.avisosSincronizados = new Set(); // Rastrear quais avisos já foram sincronizados
  }

  // ─── CONEXÃO ───
  connect() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.serverUrl);
        
        this.ws.onopen = () => {
          console.log('✓ Conectado ao servidor de sincronização');
          this.isConnected = true;
          this.reconectarTentativas = 0;
          
          // Se inscrever para atualizações dessa empresa
          this.enviar({
            type: 'subscribe',
            userId: this.userId,
            empresaId: this.empresaId,
            role: this.role
          });
          
          this.emit('conectado');
          resolve();
        };
        
        this.ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            this.processarMensagem(msg);
          } catch (err) {
            console.error('❌ Erro processando mensagem:', err);
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

  // ─── AÇÕES AVISOS (CRUD) ───
  criarAviso(aviso) {
    const id = aviso.id || 'aviso_' + Date.now();
    this.avisosSincronizados.add(id);
    
    this.enviar({
      type: 'criar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      aviso: {
        id,
        tipo: aviso.tipo || 'anotacao',
        texto: aviso.texto || '',
        data: aviso.data || new Date().toLocaleDateString('pt-BR'),
        feito: aviso.feito || false,
        visualizado: false,
        criadoEm: new Date().toISOString()
      }
    });
  }

  atualizarAviso(avisoId, updates) {
    this.enviar({
      type: 'atualizar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      avisoId,
      updates: {
        ...updates,
        atualizadoEm: new Date().toISOString()
      }
    });
  }

  marcarComoVisualizado(avisoId) {
    this.atualizarAviso(avisoId, { visualizado: true });
  }

  deletarAviso(avisoId) {
    this.enviar({
      type: 'deletar-aviso',
      userId: this.userId,
      empresaId: this.empresaId,
      avisoId
    });
  }

  // ─── AÇÕES COLUNAS ───
  atualizarColunas(colunas) {
    // Apenas admins podem fazer isso
    if (this.role !== 'admin') {
      console.warn('❌ Apenas admins podem modificar colunas');
      return;
    }

    this.enviar({
      type: 'atualizar-colunas',
      userId: this.userId,
      empresaId: this.empresaId,
      colunas: colunas,
      atualizadoEm: new Date().toISOString()
    });
  }

  // ─── PROCESSAMENTO DE MENSAGENS ───
  processarMensagem(msg) {
    switch(msg.type) {
      case 'avisos-carregados':
        this.emit('avisos-carregados', msg.avisos);
        break;
      
      case 'aviso-criado':
        if (msg.empresaId === this.empresaId && msg.aviso.id) {
          // Marcar como já sincronizado localmente
          this.avisosSincronizados.add(msg.aviso.id);
          this.emit('aviso-criado', msg.aviso);
        }
        break;
      
      case 'aviso-atualizado':
        if (msg.empresaId === this.empresaId) {
          this.emit('aviso-atualizado', msg.avisoId, msg.updates);
          
          // Se marcou como feito/visualizado, remover da exibição
          if (msg.updates.feito || msg.updates.visualizado) {
            this.emit('aviso-resolvido', msg.avisoId);
          }
        }
        break;
      
      case 'aviso-deletado':
        if (msg.empresaId === this.empresaId) {
          this.emit('aviso-deletado', msg.avisoId);
        }
        break;

      case 'colunas-atualizadas':
        if (msg.empresaId === this.empresaId) {
          console.log('🔄 Colunas atualizadas pelo admin:', msg.userId);
          this.emit('colunas-atualizadas', msg.colunas, msg.userId);
        }
        break;

      case 'permissao-negada':
        this.emit('erro', new Error(msg.mensagem || 'Permissão negada'));
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
          console.error(`❌ Erro em listener ${evento}:`, err);
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
