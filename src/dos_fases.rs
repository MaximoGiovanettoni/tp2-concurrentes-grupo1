//! PARTE 3 — Two-Phase Commit (2PC) para la compra de una entrada.
//!
//! Participantes (hilos con buzón, como actores): `ServicioPagos` e `ServicioInventario`.
//! Coordinador (`coordinar_2pc`):
//!   FASE 1 (Prepare / votación): manda PREPARE a todos y junta los votos con timeout.
//!       Cada participante que vota YES reserva el recurso (fondos / asiento) y queda
//!       PREPARADO: ya NO puede decidir solo, debe obedecer al coordinador.
//!   FASE 2 (Commit / Rollback): si TODOS votaron YES -> COMMIT; si alguien votó NO o no
//!       contestó a tiempo -> ROLLBACK.
//! Cada participante lleva un WAL (log de eventos) para mostrar el recorrido de estados.

use std::collections::HashMap;
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub type TxId = u64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Voto {
    Yes,
    No,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Decision {
    Commit,
    Rollback,
}

pub enum MsgParticipante {
    Prepare { tx: TxId, responder_a: Sender<(String, Voto)> },
    Commit { tx: TxId },
    Rollback { tx: TxId },
    Detener,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstadoParticipante {
    Inicial,
    Preparado,
    Confirmado,
    Abortado,
}

/// Fallas inyectables en un participante.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Comportamiento {
    Normal,
    /// Se cae antes de votar: no responde nada (silencio).
    NoResponde,
}

/// Fallas inyectables en el coordinador.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FallaCoordinador {
    Ninguna,
    /// Se cae después de recibir los votos y antes de enviar la decisión.
    CaeTrasPrepare,
}

// ---------------------------------------------------------------------------
// Recursos que gestionan los servicios
// ---------------------------------------------------------------------------

pub trait Recurso: Send + 'static {
    /// Reserva tentativa. YES si pudo, NO si no.
    fn preparar(&mut self) -> Voto;
    fn confirmar(&mut self);
    fn revertir(&mut self);
    fn retenido(&self) -> bool;
    fn resumen(&self) -> String;
}

pub struct ServicioPagos {
    pub saldo: u64,
    pub monto: u64,
    pub retenido: bool,
}

impl Recurso for ServicioPagos {
    fn preparar(&mut self) -> Voto {
        if !self.retenido && self.saldo >= self.monto {
            self.saldo -= self.monto; // los fondos quedan retenidos
            self.retenido = true;
            Voto::Yes
        } else {
            Voto::No
        }
    }
    fn confirmar(&mut self) {
        self.retenido = false; // el débito pasa a definitivo
    }
    fn revertir(&mut self) {
        if self.retenido {
            self.saldo += self.monto;
            self.retenido = false;
        }
    }
    fn retenido(&self) -> bool {
        self.retenido
    }
    fn resumen(&self) -> String {
        format!("saldo={} fondos_retenidos={}", self.saldo, self.retenido)
    }
}

pub struct ServicioInventario {
    pub asiento_libre: bool,
    pub retenido: bool,
    pub vendido: bool,
}

impl Recurso for ServicioInventario {
    fn preparar(&mut self) -> Voto {
        if self.asiento_libre && !self.retenido {
            self.asiento_libre = false; // el asiento queda bloqueado
            self.retenido = true;
            Voto::Yes
        } else {
            Voto::No
        }
    }
    fn confirmar(&mut self) {
        self.retenido = false;
        self.vendido = true;
    }
    fn revertir(&mut self) {
        if self.retenido {
            self.asiento_libre = true;
            self.retenido = false;
        }
    }
    fn retenido(&self) -> bool {
        self.retenido
    }
    fn resumen(&self) -> String {
        format!("asiento_libre={} asiento_retenido={} vendido={}", self.asiento_libre, self.retenido, self.vendido)
    }
}

// ---------------------------------------------------------------------------
// Participante
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Reporte {
    pub nombre: String,
    pub estado: EstadoParticipante,
    /// true si quedó PREPARADO y venció el timeout sin recibir decisión (no puede decidir solo).
    pub bloqueado: bool,
    pub recurso_retenido: bool,
    pub resumen: String,
    pub wal: Vec<String>,
}

pub fn lanzar_participante<R: Recurso>(
    nombre: &str,
    mut recurso: R,
    comp: Comportamiento,
    timeout_decision: Duration,
) -> (Sender<MsgParticipante>, JoinHandle<Reporte>) {
    let (tx, rx) = channel::<MsgParticipante>();
    let nombre = nombre.to_string();
    let h = thread::spawn(move || {
        let mut estado = EstadoParticipante::Inicial;
        let mut bloqueado = false;
        let mut wal: Vec<String> = vec![];
        loop {
            let msg = if estado == EstadoParticipante::Preparado {
                match rx.recv_timeout(timeout_decision) {
                    Ok(m) => m,
                    Err(RecvTimeoutError::Timeout) => {
                        if !bloqueado {
                            bloqueado = true;
                            wal.push(
                                "TIMEOUT esperando decisión: estoy PREPARADO, no puedo hacer commit ni abort por mi cuenta -> BLOQUEADO"
                                    .to_string(),
                            );
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            } else {
                match rx.recv() {
                    Ok(m) => m,
                    Err(_) => break,
                }
            };
            if comp == Comportamiento::NoResponde && !matches!(msg, MsgParticipante::Detener) {
                wal.push("(caído) mensaje ignorado".to_string());
                continue;
            }
            match msg {
                MsgParticipante::Prepare { tx, responder_a } => {
                    let voto = recurso.preparar();
                    if voto == Voto::Yes {
                        estado = EstadoParticipante::Preparado;
                        wal.push(format!("tx {}: PREPARED (recurso reservado) -> voto YES", tx));
                    } else {
                        estado = EstadoParticipante::Abortado;
                        wal.push(format!("tx {}: no puedo reservar -> ABORT local, voto NO", tx));
                    }
                    let _ = responder_a.send((nombre.clone(), voto));
                }
                MsgParticipante::Commit { tx } => {
                    if estado == EstadoParticipante::Preparado {
                        recurso.confirmar();
                        estado = EstadoParticipante::Confirmado;
                        wal.push(format!("tx {}: COMMIT aplicado", tx));
                    }
                }
                MsgParticipante::Rollback { tx } => {
                    if estado != EstadoParticipante::Confirmado {
                        recurso.revertir();
                        estado = EstadoParticipante::Abortado;
                        wal.push(format!("tx {}: ROLLBACK aplicado (recurso liberado)", tx));
                    }
                }
                MsgParticipante::Detener => break,
            }
        }
        Reporte {
            nombre,
            estado,
            bloqueado,
            recurso_retenido: recurso.retenido(),
            resumen: recurso.resumen(),
            wal,
        }
    });
    (tx, h)
}

// ---------------------------------------------------------------------------
// Coordinador
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Resultado {
    /// None si el coordinador cayó antes de decidir.
    pub decision: Option<Decision>,
    pub votos: Vec<(String, Option<Voto>)>,
    pub log: Vec<String>,
}

pub fn coordinar_2pc(
    tx: TxId,
    participantes: &[(String, Sender<MsgParticipante>)],
    timeout_votos: Duration,
    falla: FallaCoordinador,
) -> Resultado {
    let mut log = vec![];

    // ---------------- FASE 1: PREPARE ----------------
    log.push(format!("[FASE 1 - PREPARE] tx {}: envío PREPARE a {} participantes", tx, participantes.len()));
    let (tx_votos, rx_votos) = channel::<(String, Voto)>();
    for (_, p) in participantes {
        let _ = p.send(MsgParticipante::Prepare { tx, responder_a: tx_votos.clone() });
    }
    let limite = Instant::now() + timeout_votos;
    let mut votos: HashMap<String, Voto> = HashMap::new();
    while votos.len() < participantes.len() {
        let restante = limite.saturating_duration_since(Instant::now());
        match rx_votos.recv_timeout(restante) {
            Ok((nombre, v)) => {
                log.push(format!("  voto de {}: {:?}", nombre, v));
                votos.insert(nombre, v);
            }
            Err(RecvTimeoutError::Timeout) => {
                log.push("  TIMEOUT esperando votos: los que faltan se cuentan como NO".to_string());
                break;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    drop(tx_votos);
    let todos_yes = participantes.iter().all(|(n, _)| votos.get(n) == Some(&Voto::Yes));
    let votos_ordenados: Vec<_> = participantes.iter().map(|(n, _)| (n.clone(), votos.get(n).copied())).collect();

    // Falla del coordinador: los votos ya están, pero nunca sale la decisión.
    if falla == FallaCoordinador::CaeTrasPrepare {
        log.push("  *** EL COORDINADOR SE CAE antes de decidir: nadie envía COMMIT ni ROLLBACK ***".to_string());
        return Resultado { decision: None, votos: votos_ordenados, log };
    }

    // ---------------- FASE 2: COMMIT / ROLLBACK ----------------
    let decision = if todos_yes { Decision::Commit } else { Decision::Rollback };
    log.push(format!(
        "[FASE 2 - {}] decisión registrada en log durable ANTES de enviarla",
        if todos_yes { "COMMIT" } else { "ROLLBACK" }
    ));
    for (nombre, p) in participantes {
        let msg = match decision {
            Decision::Commit => MsgParticipante::Commit { tx },
            Decision::Rollback => MsgParticipante::Rollback { tx },
        };
        log.push(format!("  envío {:?} a {}", decision, nombre));
        let _ = p.send(msg);
    }
    Resultado { decision: Some(decision), votos: votos_ordenados, log }
}

// ---------------------------------------------------------------------------
// Escenarios de prueba (los usan main y los tests)
// ---------------------------------------------------------------------------

pub struct Escenario {
    pub saldo: u64,
    pub monto: u64,
    pub asiento_libre: bool,
    pub pagos: Comportamiento,
    pub inventario: Comportamiento,
    pub falla: FallaCoordinador,
}

impl Escenario {
    pub fn feliz() -> Self {
        Escenario {
            saldo: 1000,
            monto: 50,
            asiento_libre: true,
            pagos: Comportamiento::Normal,
            inventario: Comportamiento::Normal,
            falla: FallaCoordinador::Ninguna,
        }
    }
}

/// Ejecuta una compra completa con un 2PC. Devuelve el resultado del coordinador y los
/// reportes finales de [Pagos, Inventario].
pub fn ejecutar_escenario(e: Escenario, timeout: Duration) -> (Resultado, Vec<Reporte>) {
    let (tx_p, h_p) = lanzar_participante(
        "ServicioPagos",
        ServicioPagos { saldo: e.saldo, monto: e.monto, retenido: false },
        e.pagos,
        timeout * 2, // el participante espera más que el coordinador: si vence, es un bloqueo real
    );
    let (tx_i, h_i) = lanzar_participante(
        "ServicioInventario",
        ServicioInventario { asiento_libre: e.asiento_libre, retenido: false, vendido: false },
        e.inventario,
        timeout * 2,
    );
    let participantes = vec![("ServicioPagos".to_string(), tx_p.clone()), ("ServicioInventario".to_string(), tx_i.clone())];
    let resultado = coordinar_2pc(1, &participantes, timeout, e.falla);
    if resultado.decision.is_none() {
        // Dejamos correr más que el timeout para que los participantes preparados
        // detecten que quedaron bloqueados.
        thread::sleep(timeout * 4);
    }
    let _ = tx_p.send(MsgParticipante::Detener);
    let _ = tx_i.send(MsgParticipante::Detener);
    let reportes = vec![h_p.join().unwrap(), h_i.join().unwrap()];
    (resultado, reportes)
}

#[cfg(test)]
mod tests {
    use super::*;
    const T: Duration = Duration::from_millis(150);

    #[test]
    fn todos_yes_hace_commit() {
        let (r, rep) = ejecutar_escenario(Escenario::feliz(), T);
        assert_eq!(r.decision, Some(Decision::Commit));
        assert!(rep.iter().all(|x| x.estado == EstadoParticipante::Confirmado && !x.bloqueado));
        assert!(rep[0].resumen.contains("saldo=950"));
        assert!(rep[1].resumen.contains("vendido=true"));
    }

    #[test]
    fn un_no_en_inventario_hace_rollback_y_libera_fondos() {
        let e = Escenario { asiento_libre: false, ..Escenario::feliz() };
        let (r, rep) = ejecutar_escenario(e, T);
        assert_eq!(r.decision, Some(Decision::Rollback));
        assert_eq!(rep[0].estado, EstadoParticipante::Abortado);
        assert!(rep[0].resumen.contains("saldo=1000"), "{}", rep[0].resumen);
        assert!(!rep[0].recurso_retenido);
    }

    #[test]
    fn saldo_insuficiente_hace_rollback_y_libera_asiento() {
        let e = Escenario { saldo: 10, ..Escenario::feliz() };
        let (r, rep) = ejecutar_escenario(e, T);
        assert_eq!(r.decision, Some(Decision::Rollback));
        assert!(rep[1].resumen.contains("asiento_libre=true"), "{}", rep[1].resumen);
        assert!(!rep[1].recurso_retenido);
    }

    #[test]
    fn participante_que_no_responde_provoca_rollback_por_timeout() {
        let e = Escenario { pagos: Comportamiento::NoResponde, ..Escenario::feliz() };
        let (r, rep) = ejecutar_escenario(e, T);
        assert_eq!(r.decision, Some(Decision::Rollback));
        assert_eq!(r.votos[0].1, None);
        assert_eq!(rep[1].estado, EstadoParticipante::Abortado);
        assert!(!rep[1].recurso_retenido);
    }

    #[test]
    fn coordinador_cae_tras_prepare_bloquea_a_los_participantes() {
        let e = Escenario { falla: FallaCoordinador::CaeTrasPrepare, ..Escenario::feliz() };
        let (r, rep) = ejecutar_escenario(e, T);
        assert_eq!(r.decision, None);
        for x in &rep {
            assert_eq!(x.estado, EstadoParticipante::Preparado);
            assert!(x.bloqueado, "{} debería estar bloqueado", x.nombre);
            assert!(x.recurso_retenido, "{} sigue reteniendo su recurso", x.nombre);
        }
    }
}
