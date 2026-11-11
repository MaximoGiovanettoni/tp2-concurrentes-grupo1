//! PARTE 2.2 — Algoritmo de Bully (elección de líder), simulado con mensajes.
//!
//! Regla: gana el nodo VIVO con mayor id. Mensajes:
//!   Eleccion{de}      "Quiero ser líder, ¿hay alguien con id mayor?"
//!   Respuesta{de}     "Yo (id mayor) estoy vivo, vos abandonás la elección" (el OK del paper)
//!   Coordinador{lider} "Soy el nuevo líder"
//! Un nodo caído simplemente no contesta (se detecta por timeout).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MsgBully {
    Eleccion { de: usize },
    Respuesta { de: usize },
    Coordinador { lider: usize },
    Detener,
}

#[derive(Debug)]
pub struct ResultadoEleccion {
    /// Líder que cree cada nodo al final (None para los caídos).
    pub lideres: Vec<Option<usize>>,
    pub mensajes: usize,
}

enum Fase {
    Inactivo,
    EsperandoRespuesta(Instant),
    EsperandoCoordinador(Instant),
}

fn enviar(pares: &[Sender<MsgBully>], a: usize, msg: MsgBully, contador: &AtomicUsize) {
    contador.fetch_add(1, Ordering::SeqCst);
    let _ = pares[a].send(msg);
}

fn iniciar_eleccion(id: usize, pares: &[Sender<MsgBully>], espera: Duration, c: &AtomicUsize, v: bool) -> Fase {
    if v {
        println!("  Nodo {}: inicio elección -> mando ELECCION a los nodos con id mayor", id);
    }
    for j in (id + 1)..pares.len() {
        enviar(pares, j, MsgBully::Eleccion { de: id }, c);
    }
    Fase::EsperandoRespuesta(Instant::now() + espera)
}

#[allow(clippy::too_many_arguments)]
fn nodo(
    id: usize,
    inbox: Receiver<MsgBully>,
    pares: Vec<Sender<MsgBully>>,
    vivo: bool,
    iniciar: bool,
    espera: Duration,
    contador: Arc<AtomicUsize>,
    resultado: Sender<(usize, Option<usize>)>,
    v: bool,
) {
    let n = pares.len();
    let mut lider = None;
    let mut fase = Fase::Inactivo;
    if vivo && iniciar {
        fase = iniciar_eleccion(id, &pares, espera, &contador, v);
    }
    loop {
        let wait = match fase {
            Fase::Inactivo => Duration::from_millis(20),
            Fase::EsperandoRespuesta(d) | Fase::EsperandoCoordinador(d) => {
                d.saturating_duration_since(Instant::now())
            }
        };
        match inbox.recv_timeout(wait) {
            Ok(MsgBully::Detener) | Err(RecvTimeoutError::Disconnected) => break,
            // Un nodo caído ignora todo: para los demás es "silencio".
            Ok(_) if !vivo => {}
            Ok(MsgBully::Eleccion { de }) => {
                if v {
                    println!("  Nodo {}: recibí ELECCION de {} -> respondo y arranco mi propia elección", id, de);
                }
                enviar(&pares, de, MsgBully::Respuesta { de: id }, &contador);
                if matches!(fase, Fase::Inactivo) {
                    fase = iniciar_eleccion(id, &pares, espera, &contador, v);
                }
            }
            Ok(MsgBully::Respuesta { .. }) => {
                if matches!(fase, Fase::EsperandoRespuesta(_)) {
                    // Hay alguien mayor vivo: me retiro y espero su anuncio.
                    fase = Fase::EsperandoCoordinador(Instant::now() + espera * 3);
                }
            }
            Ok(MsgBully::Coordinador { lider: l }) => {
                if v {
                    println!("  Nodo {}: acepto como líder al nodo {}", id, l);
                }
                lider = Some(l);
                fase = Fase::Inactivo;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !vivo {
                    continue;
                }
                match fase {
                    Fase::EsperandoRespuesta(d) if Instant::now() >= d => {
                        // Nadie mayor contestó: soy el líder.
                        if v {
                            println!("  Nodo {}: nadie con id mayor respondió -> SOY EL NUEVO LÍDER", id);
                        }
                        lider = Some(id);
                        for j in 0..n {
                            if j != id {
                                enviar(&pares, j, MsgBully::Coordinador { lider: id }, &contador);
                            }
                        }
                        fase = Fase::Inactivo;
                    }
                    Fase::EsperandoCoordinador(d) if Instant::now() >= d => {
                        fase = iniciar_eleccion(id, &pares, espera, &contador, v);
                    }
                    _ => {}
                }
            }
        }
    }
    let _ = resultado.send((id, if vivo { lider } else { None }));
}

/// Simula una elección con `n` nodos. `caidos` no responden; `iniciador` es quien detectó la falla.
pub fn simular_eleccion(
    n: usize,
    caidos: &[usize],
    iniciador: usize,
    espera: Duration,
    verboso: bool,
) -> ResultadoEleccion {
    let contador = Arc::new(AtomicUsize::new(0));
    let (tx_res, rx_res) = channel();
    let (buzones_tx, buzones_rx): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::<MsgBully>()).unzip();
    let mut hilos = vec![];
    for (id, rx) in buzones_rx.into_iter().enumerate() {
        let pares = buzones_tx.clone();
        let vivo = !caidos.contains(&id);
        let c = Arc::clone(&contador);
        let r = tx_res.clone();
        hilos.push(thread::spawn(move || {
            nodo(id, rx, pares, vivo, id == iniciador, espera, c, r, verboso)
        }));
    }
    drop(tx_res);
    thread::sleep(espera * 8);
    for tx in &buzones_tx {
        let _ = tx.send(MsgBully::Detener);
    }
    let mut lideres = vec![None; n];
    for (id, l) in rx_res.iter() {
        lideres[id] = l;
    }
    for h in hilos {
        h.join().unwrap();
    }
    ResultadoEleccion { lideres, mensajes: contador.load(Ordering::SeqCst) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cae_el_lider_y_gana_el_mayor_vivo() {
        // Nodos 0,1,2; líder era el 2 (mayor id) y cayó; detecta el 0.
        let r = simular_eleccion(3, &[2], 0, Duration::from_millis(40), false);
        assert_eq!(r.lideres, vec![Some(1), Some(1), None]);
    }

    #[test]
    fn cinco_nodos_caen_los_dos_mayores() {
        let r = simular_eleccion(5, &[3, 4], 0, Duration::from_millis(40), false);
        assert_eq!(r.lideres, vec![Some(2), Some(2), Some(2), None, None]);
    }

    #[test]
    fn si_el_mayor_esta_vivo_gana_aunque_inicie_otro() {
        let r = simular_eleccion(3, &[], 0, Duration::from_millis(40), false);
        assert_eq!(r.lideres, vec![Some(2), Some(2), Some(2)]);
    }
}
