//! PARTE 2.1 — Detección de fallos con Heartbeats.
//!
//! 3 nodos envían un `Heartbeat` cada 500 ms por un canal `mpsc`. Por cada nodo hay un
//! vigilante del Monitor que usa `recv_timeout`:
//!   - sin latido por 1 s  -> SOSPECHOSO
//!   - sin latido por 2 s  -> CAIDO (1 s más tarde, desde el último latido)
//! Si llega un latido estando SOSPECHOSO, el nodo vuelve a VIVO (falsa alarma).
//!
//! La caída se simula con un flag (`caido`): el nodo deja de enviar pero NO cierra el canal,
//! igual que un servidor colgado o una red partida (el monitor no recibe ningún aviso, solo silencio).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstadoNodo {
    Vivo,
    Sospechoso,
    Caido,
}

#[derive(Debug, Clone, Copy)]
pub struct Heartbeat {
    pub nodo: usize,
    pub secuencia: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Evento {
    Sospechoso(usize),
    Caido(usize),
    Recuperado(usize),
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub periodo: Duration,
    pub umbral_sospechoso: Duration,
    pub umbral_caido: Duration,
}

impl Config {
    /// Valores del enunciado: 500 ms / 1 s / 2 s.
    pub fn enunciado() -> Self {
        Config {
            periodo: Duration::from_millis(500),
            umbral_sospechoso: Duration::from_secs(1),
            umbral_caido: Duration::from_secs(2),
        }
    }
    /// Misma lógica con tiempos 10 veces menores (para tests rápidos).
    pub fn rapida() -> Self {
        Config {
            periodo: Duration::from_millis(50),
            umbral_sospechoso: Duration::from_millis(100),
            umbral_caido: Duration::from_millis(200),
        }
    }
}

/// Elige un nodo "aleatorio" sin dependencias externas (nanosegundos del reloj).
pub fn nodo_aleatorio(n: usize) -> usize {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as usize % n
}

fn lanzar_nodo(
    id: usize,
    tx: Sender<Heartbeat>,
    cfg: Config,
    caido: Arc<AtomicBool>,
    apagar: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut secuencia = 0;
        while !apagar.load(Ordering::SeqCst) {
            if !caido.load(Ordering::SeqCst) {
                if tx.send(Heartbeat { nodo: id, secuencia }).is_err() {
                    break;
                }
                secuencia += 1;
            }
            thread::sleep(cfg.periodo);
        }
    })
}

/// Vigilante de un nodo: máquina de estados Vivo -> Sospechoso -> Caido con `recv_timeout`.
pub fn vigilar(id: usize, rx: Receiver<Heartbeat>, cfg: Config, eventos: Sender<Evento>) {
    let mut estado = EstadoNodo::Vivo;
    loop {
        // Cuánto esperar depende del estado: 1 s hasta sospechar y 1 s más hasta declarar caído.
        let espera = match estado {
            EstadoNodo::Vivo => cfg.umbral_sospechoso,
            EstadoNodo::Sospechoso => cfg.umbral_caido - cfg.umbral_sospechoso,
            EstadoNodo::Caido => return,
        };
        match rx.recv_timeout(espera) {
            Ok(_hb) => {
                if estado == EstadoNodo::Sospechoso {
                    let _ = eventos.send(Evento::Recuperado(id));
                }
                estado = EstadoNodo::Vivo;
            }
            Err(RecvTimeoutError::Timeout) => {
                if estado == EstadoNodo::Vivo {
                    estado = EstadoNodo::Sospechoso;
                    let _ = eventos.send(Evento::Sospechoso(id));
                } else {
                    estado = EstadoNodo::Caido;
                    let _ = eventos.send(Evento::Caido(id));
                }
            }
            // El nodo terminó limpiamente (cierre del sistema): no es una falla.
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Corre `n_nodos` nodos + monitor. A los `caer_tras` el nodo `victima` deja de latir.
/// Termina cuando el monitor declara CAIDO a la víctima. Devuelve los eventos con su instante.
pub fn simular(
    cfg: Config,
    n_nodos: usize,
    victima: usize,
    caer_tras: Duration,
    imprimir: bool,
) -> Vec<(Duration, Evento)> {
    let inicio = Instant::now();
    let apagar = Arc::new(AtomicBool::new(false));
    let (tx_ev, rx_ev) = channel::<Evento>();
    let mut flags = vec![];
    let mut hilos = vec![];

    for id in 0..n_nodos {
        let (tx_hb, rx_hb) = channel::<Heartbeat>();
        let caido = Arc::new(AtomicBool::new(false));
        flags.push(Arc::clone(&caido));
        hilos.push(lanzar_nodo(id, tx_hb, cfg, caido, Arc::clone(&apagar)));
        let tx = tx_ev.clone();
        hilos.push(thread::spawn(move || vigilar(id, rx_hb, cfg, tx)));
    }
    drop(tx_ev);

    let mut eventos = vec![];
    let mut inyectada = false;
    let limite = caer_tras + cfg.umbral_caido * 4;
    loop {
        if !inyectada && inicio.elapsed() >= caer_tras {
            flags[victima].store(true, Ordering::SeqCst);
            inyectada = true;
            if imprimir {
                println!("[{:>5} ms] *** SE CAE el nodo {} (deja de enviar heartbeats) ***",
                    inicio.elapsed().as_millis(), victima);
            }
        }
        match rx_ev.recv_timeout(cfg.periodo / 5) {
            Ok(ev) => {
                let t = inicio.elapsed();
                if imprimir {
                    println!("[{:>5} ms] Monitor: {:?}", t.as_millis(), ev);
                }
                eventos.push((t, ev));
                if ev == Evento::Caido(victima) {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if inicio.elapsed() > limite {
            break;
        }
    }
    apagar.store(true, Ordering::SeqCst);
    for h in hilos {
        h.join().unwrap();
    }
    eventos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodo_caido_pasa_por_sospechoso_y_luego_caido() {
        let cfg = Config::rapida();
        let ev = simular(cfg, 3, 1, Duration::from_millis(300), false);
        let pos_s = ev.iter().position(|(_, e)| *e == Evento::Sospechoso(1)).expect("sospechoso");
        let pos_c = ev.iter().position(|(_, e)| *e == Evento::Caido(1)).expect("caido");
        assert!(pos_s < pos_c, "primero SOSPECHOSO y después CAIDO");
        // Los otros nodos nunca se declaran caídos.
        assert!(ev.iter().all(|(_, e)| !matches!(e, Evento::Caido(n) if *n != 1)));
    }

    #[test]
    fn nodo_sano_no_genera_eventos() {
        // Si nadie cae (víctima fuera de rango de tiempo), el monitor no declara nada.
        let cfg = Config::rapida();
        let (tx, rx) = channel();
        let (txe, rxe) = channel();
        let h = thread::spawn(move || vigilar(0, rx, cfg, txe));
        for i in 0..5 {
            tx.send(Heartbeat { nodo: 0, secuencia: i }).unwrap();
            thread::sleep(cfg.periodo);
        }
        drop(tx);
        h.join().unwrap();
        assert!(rxe.try_recv().is_err());
    }

    #[test]
    fn falsa_alarma_se_recupera() {
        let cfg = Config::rapida();
        let (tx, rx) = channel();
        let (txe, rxe) = channel();
        let h = thread::spawn(move || vigilar(0, rx, cfg, txe));
        thread::sleep(Duration::from_millis(150)); // > 100 ms (sospechoso) y < 200 ms (caído)
        tx.send(Heartbeat { nodo: 0, secuencia: 0 }).unwrap();
        thread::sleep(Duration::from_millis(30));
        drop(tx);
        h.join().unwrap();
        let ev: Vec<_> = rxe.try_iter().collect();
        assert_eq!(ev, vec![Evento::Sospechoso(0), Evento::Recuperado(0)]);
    }
}
