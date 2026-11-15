use std::sync::mpsc::{channel, Sender};
use std::thread;

pub enum MensajeCuenta {
    ConfirmarDebito {
        monto: u64,
        canal_respuesta: Sender<RespuestaDebito>,
    },
    ConsultarSaldo {
        canal_respuesta: Sender<u64>,
    },
}

pub enum RespuestaDebito {
    Ok,
    FondosInsuficientes,
}

/// Abre el buzón, lanza el hilo y devuelve la dirección del actor.
/// El saldo es una variable de ese hilo: nadie más la tiene.
pub fn crear_actor_cuenta(saldo_inicial: u64) -> Sender<MensajeCuenta> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut saldo = saldo_inicial;
        for msg in rx {
            match msg {
                MensajeCuenta::ConfirmarDebito {
                    monto,
                    canal_respuesta,
                } => {
                    if saldo >= monto {
                        saldo -= monto;
                        let _ = canal_respuesta.send(RespuestaDebito::Ok);
                    } else {
                        let _ = canal_respuesta.send(RespuestaDebito::FondosInsuficientes);
                    }
                }
                MensajeCuenta::ConsultarSaldo { canal_respuesta } => {
                    let _ = canal_respuesta.send(saldo);
                }
            }
        }
    });
    tx
}

pub enum MensajeInventario {
    SolicitarReserva {
        canal_respuesta: Sender<RespuestaReserva>,
    },
    ConsultarDisponibles {
        canal_respuesta: Sender<u32>,
    },
}

pub enum RespuestaReserva {
    Ok,
    Agotado,
}

/// Igual que la cuenta: las entradas disponibles viven solo en este hilo.
pub fn crear_actor_inventario(entradas: u32) -> Sender<MensajeInventario> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut disponibles = entradas;
        for msg in rx {
            match msg {
                MensajeInventario::SolicitarReserva { canal_respuesta } => {
                    if disponibles > 0 {
                        disponibles -= 1;
                        let _ = canal_respuesta.send(RespuestaReserva::Ok);
                    } else {
                        let _ = canal_respuesta.send(RespuestaReserva::Agotado);
                    }
                }
                MensajeInventario::ConsultarDisponibles { canal_respuesta } => {
                    let _ = canal_respuesta.send(disponibles);
                }
            }
        }
    });
    tx
}

/// Varios hilos le piden el mismo débito a una sola cuenta.
/// Devuelve cuántos entraron y el saldo que queda.
pub fn simular_debitos(saldo: u64, monto: u64, hilos: usize) -> (usize, u64) {
    let cuenta = crear_actor_cuenta(saldo);
    let mut handles = Vec::new();
    for _ in 0..hilos {
        let cuenta = cuenta.clone();
        handles.push(thread::spawn(move || {
            let (tx, rx) = channel();
            cuenta
                .send(MensajeCuenta::ConfirmarDebito {
                    monto,
                    canal_respuesta: tx,
                })
                .unwrap();
            matches!(rx.recv().unwrap(), RespuestaDebito::Ok)
        }));
    }
    let mut ok = 0;
    for h in handles {
        if h.join().unwrap() {
            ok += 1;
        }
    }
    let (tx, rx) = channel();
    cuenta
        .send(MensajeCuenta::ConsultarSaldo {
            canal_respuesta: tx,
        })
        .unwrap();
    (ok, rx.recv().unwrap())
}

/// Varios hilos piden una entrada cada uno. Devuelve cuántos la consiguieron
/// y cuántas quedan.
pub fn simular_reservas(entradas: u32, hilos: usize) -> (usize, u32) {
    let inventario = crear_actor_inventario(entradas);
    let mut handles = Vec::new();
    for _ in 0..hilos {
        let inventario = inventario.clone();
        handles.push(thread::spawn(move || {
            let (tx, rx) = channel();
            inventario
                .send(MensajeInventario::SolicitarReserva {
                    canal_respuesta: tx,
                })
                .unwrap();
            matches!(rx.recv().unwrap(), RespuestaReserva::Ok)
        }));
    }
    let mut ok = 0;
    for h in handles {
        if h.join().unwrap() {
            ok += 1;
        }
    }
    let (tx, rx) = channel();
    inventario
        .send(MensajeInventario::ConsultarDisponibles {
            canal_respuesta: tx,
        })
        .unwrap();
    (ok, rx.recv().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debitos_concurrentes_no_sobregiran() {
        // 50 hilos piden 30 sobre 1000: entran 33 (990) y sobran 10.
        let (ok, saldo) = simular_debitos(1000, 30, 50);
        assert_eq!(ok, 33);
        assert_eq!(saldo, 10);
    }

    #[test]
    fn debito_sin_fondos_no_toca_el_saldo() {
        let cuenta = crear_actor_cuenta(30);
        let (tx, rx) = channel();
        cuenta
            .send(MensajeCuenta::ConfirmarDebito {
                monto: 40,
                canal_respuesta: tx,
            })
            .unwrap();
        assert!(matches!(rx.recv().unwrap(), RespuestaDebito::FondosInsuficientes));
        let (tx, rx) = channel();
        cuenta
            .send(MensajeCuenta::ConsultarSaldo {
                canal_respuesta: tx,
            })
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 30);
    }

    #[test]
    fn un_solo_hilo_se_queda_con_la_ultima_entrada() {
        let (ok, libres) = simular_reservas(1, 20);
        assert_eq!(ok, 1);
        assert_eq!(libres, 0);
    }
}
