//! PARTE 1 — Modelo de Actores.
//!
//! Cada actor es un hilo que es DUEÑO de su estado (nadie más lo toca) y lo único
//! que hace es leer su buzón (`Receiver`) y procesar UN mensaje por vez.
//! Como el estado nunca se comparte, no hay `Mutex` sobre el saldo ni sobre los asientos:
//! la serialización de los accesos la da el buzón (cola FIFO de mensajes).

use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::thread::{self, JoinHandle};

pub type UsuarioId = u32;
pub type AsientoId = u32;

// ---------------------------------------------------------------------------
// Actor CuentaUsuario
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorCuenta {
    SaldoInsuficiente { saldo: u64, pedido: u64 },
}

/// Mensajes que acepta el actor `CuentaUsuario`.
/// Los que esperan respuesta llevan un `Sender` por donde contestar.
pub enum MsgCuenta {
    ConsultarSaldo { responder_a: Sender<u64> },
    /// `ConfirmarDebito` del enunciado: descuenta `monto` si alcanza el saldo.
    ConfirmarDebito { monto: u64, responder_a: Sender<Result<u64, ErrorCuenta>> },
    /// Compensación: devuelve plata (rollback de un débito ya hecho).
    Acreditar { monto: u64 },
    Detener,
}

/// Estado PRIVADO del actor (los campos no son `pub`).
pub struct CuentaUsuario {
    #[allow(dead_code)]
    id: UsuarioId,
    saldo: u64,
}

impl CuentaUsuario {
    /// Lanza el actor. Devuelve su dirección (`Sender`) y el handle, que al
    /// terminar entrega el saldo final.
    pub fn iniciar(id: UsuarioId, saldo: u64) -> (Sender<MsgCuenta>, JoinHandle<u64>) {
        let (tx, rx) = channel::<MsgCuenta>();
        let handle = thread::spawn(move || {
            let mut cuenta = CuentaUsuario { id, saldo };
            // Bucle del actor: un mensaje a la vez, sin locks.
            for msg in rx {
                match msg {
                    MsgCuenta::ConsultarSaldo { responder_a } => {
                        let _ = responder_a.send(cuenta.saldo);
                    }
                    MsgCuenta::ConfirmarDebito { monto, responder_a } => {
                        let r = if cuenta.saldo >= monto {
                            cuenta.saldo -= monto;
                            Ok(cuenta.saldo)
                        } else {
                            Err(ErrorCuenta::SaldoInsuficiente { saldo: cuenta.saldo, pedido: monto })
                        };
                        let _ = responder_a.send(r);
                    }
                    MsgCuenta::Acreditar { monto } => cuenta.saldo += monto,
                    MsgCuenta::Detener => break,
                }
            }
            cuenta.saldo
        });
        (tx, handle)
    }
}

// ---------------------------------------------------------------------------
// Actor InventarioEntradas
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstadoAsiento {
    Libre,
    Reservado(UsuarioId),
    Vendido(UsuarioId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorInventario {
    AsientoInexistente,
    AsientoNoDisponible,
    ReservaInvalida,
}

/// Mensajes que acepta el actor `InventarioEntradas`.
pub enum MsgInventario {
    SolicitarReserva {
        asiento: AsientoId,
        usuario: UsuarioId,
        responder_a: Sender<Result<(), ErrorInventario>>,
    },
    ConfirmarVenta {
        asiento: AsientoId,
        usuario: UsuarioId,
        responder_a: Sender<Result<(), ErrorInventario>>,
    },
    LiberarReserva { asiento: AsientoId, usuario: UsuarioId },
    Consultar { responder_a: Sender<HashMap<AsientoId, EstadoAsiento>> },
    Detener,
}

pub struct InventarioEntradas {
    asientos: HashMap<AsientoId, EstadoAsiento>,
}

impl InventarioEntradas {
    /// Lanza el actor con `cantidad` asientos libres (ids 1..=cantidad).
    /// El handle entrega el mapa final de asientos.
    pub fn iniciar(cantidad: u32) -> (Sender<MsgInventario>, JoinHandle<HashMap<AsientoId, EstadoAsiento>>) {
        let (tx, rx) = channel::<MsgInventario>();
        let handle = thread::spawn(move || {
            let mut inv = InventarioEntradas {
                asientos: (1..=cantidad).map(|a| (a, EstadoAsiento::Libre)).collect(),
            };
            for msg in rx {
                match msg {
                    MsgInventario::SolicitarReserva { asiento, usuario, responder_a } => {
                        // Chequeo + escritura son atómicos: ningún otro mensaje se
                        // procesa en el medio. Por eso es imposible vender dos veces.
                        let r = match inv.asientos.get_mut(&asiento) {
                            None => Err(ErrorInventario::AsientoInexistente),
                            Some(e @ EstadoAsiento::Libre) => {
                                *e = EstadoAsiento::Reservado(usuario);
                                Ok(())
                            }
                            Some(_) => Err(ErrorInventario::AsientoNoDisponible),
                        };
                        let _ = responder_a.send(r);
                    }
                    MsgInventario::ConfirmarVenta { asiento, usuario, responder_a } => {
                        let r = match inv.asientos.get_mut(&asiento) {
                            Some(e) if *e == EstadoAsiento::Reservado(usuario) => {
                                *e = EstadoAsiento::Vendido(usuario);
                                Ok(())
                            }
                            _ => Err(ErrorInventario::ReservaInvalida),
                        };
                        let _ = responder_a.send(r);
                    }
                    MsgInventario::LiberarReserva { asiento, usuario } => {
                        if let Some(e) = inv.asientos.get_mut(&asiento) {
                            if *e == EstadoAsiento::Reservado(usuario) {
                                *e = EstadoAsiento::Libre;
                            }
                        }
                    }
                    MsgInventario::Consultar { responder_a } => {
                        let _ = responder_a.send(inv.asientos.clone());
                    }
                    MsgInventario::Detener => break,
                }
            }
            inv.asientos
        });
        (tx, handle)
    }
}

// ---------------------------------------------------------------------------
// Flujo de compra entre actores (saga local: reservar -> debitar -> confirmar)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResultadoCompra {
    Exito,
    AsientoNoDisponible,
    SaldoInsuficiente,
}

/// Un comprador orquesta la compra hablando SOLO por mensajes con ambos actores.
pub fn comprar(
    usuario: UsuarioId,
    asiento: AsientoId,
    precio: u64,
    cuenta: &Sender<MsgCuenta>,
    inventario: &Sender<MsgInventario>,
) -> ResultadoCompra {
    // 1) SolicitarReserva
    let (tx, rx) = channel();
    inventario
        .send(MsgInventario::SolicitarReserva { asiento, usuario, responder_a: tx })
        .unwrap();
    if rx.recv().unwrap().is_err() {
        return ResultadoCompra::AsientoNoDisponible;
    }
    // 2) ConfirmarDebito
    let (tx, rx) = channel();
    cuenta.send(MsgCuenta::ConfirmarDebito { monto: precio, responder_a: tx }).unwrap();
    if rx.recv().unwrap().is_err() {
        // Compensación: se libera el asiento reservado.
        inventario.send(MsgInventario::LiberarReserva { asiento, usuario }).unwrap();
        return ResultadoCompra::SaldoInsuficiente;
    }
    // 3) ConfirmarVenta
    let (tx, rx) = channel();
    inventario
        .send(MsgInventario::ConfirmarVenta { asiento, usuario, responder_a: tx })
        .unwrap();
    if rx.recv().unwrap().is_err() {
        // No debería pasar (la reserva es nuestra), pero si pasa se devuelve la plata.
        cuenta.send(MsgCuenta::Acreditar { monto: precio }).unwrap();
        return ResultadoCompra::AsientoNoDisponible;
    }
    ResultadoCompra::Exito
}

// ---------------------------------------------------------------------------
// Simulaciones (las usan main y los tests)
// ---------------------------------------------------------------------------

/// Muchos hilos debitan de UNA MISMA cuenta a la vez. Devuelve (débitos ok, saldo final).
pub fn simular_debitos_concurrentes(saldo: u64, monto: u64, hilos: usize) -> (usize, u64) {
    let (cuenta, h_cuenta) = CuentaUsuario::iniciar(1, saldo);
    let hs: Vec<_> = (0..hilos)
        .map(|_| {
            let c = cuenta.clone();
            thread::spawn(move || {
                let (tx, rx) = channel();
                c.send(MsgCuenta::ConfirmarDebito { monto, responder_a: tx }).unwrap();
                rx.recv().unwrap().is_ok()
            })
        })
        .collect();
    let ok = hs.into_iter().map(|h| h.join().unwrap()).filter(|&b| b).count();
    cuenta.send(MsgCuenta::Detener).unwrap();
    (ok, h_cuenta.join().unwrap())
}

#[derive(Debug)]
pub struct ResumenVentas {
    pub vendidos: usize,
    pub cobrado: u64,
    pub reservas_colgadas: usize,
    pub exitos: usize,
    pub sin_asiento: usize,
    pub sin_saldo: usize,
    /// true si: nada vendido 2 veces, cobrado == vendidos*precio y no quedan reservas colgadas.
    pub consistente: bool,
}

/// `compradores` hilos compiten por `asientos` entradas. Cada tercer comprador tiene
/// saldo insuficiente (para ejercitar la compensación). Todos piden asientos que se pisan.
pub fn simular_ventas(asientos: u32, compradores: u32, saldo: u64, precio: u64) -> ResumenVentas {
    let (inv, h_inv) = InventarioEntradas::iniciar(asientos);
    let mut cuentas = vec![];
    let mut hilos = vec![];
    let mut saldos_iniciales = HashMap::new();
    for u in 1..=compradores {
        let s = if u % 3 == 0 { precio / 2 } else { saldo };
        saldos_iniciales.insert(u, s);
        let (c, h) = CuentaUsuario::iniciar(u, s);
        cuentas.push((u, c.clone(), h));
        let inv_c = inv.clone();
        hilos.push(thread::spawn(move || {
            // Todos apuntan a los mismos asientos: máxima contención.
            let asiento = (u % asientos) + 1;
            let mut r = comprar(u, asiento, precio, &c, &inv_c);
            // Si perdió el asiento, prueba con los demás hasta conseguir uno.
            let mut siguiente = asiento;
            while r == ResultadoCompra::AsientoNoDisponible {
                siguiente = (siguiente % asientos) + 1;
                if siguiente == asiento {
                    break;
                }
                r = comprar(u, siguiente, precio, &c, &inv_c);
            }
            r
        }));
    }
    let resultados: Vec<_> = hilos.into_iter().map(|h| h.join().unwrap()).collect();
    inv.send(MsgInventario::Detener).unwrap();
    let estado_final = h_inv.join().unwrap();

    let mut cobrado = 0;
    let mut saldos_finales = HashMap::new();
    for (u, c, h) in cuentas {
        c.send(MsgCuenta::Detener).unwrap();
        let fin = h.join().unwrap();
        cobrado += saldos_iniciales[&u] - fin;
        saldos_finales.insert(u, fin);
    }
    let vendidos: Vec<_> = estado_final
        .values()
        .filter_map(|e| if let EstadoAsiento::Vendido(u) = e { Some(*u) } else { None })
        .collect();
    let colgadas = estado_final.values().filter(|e| matches!(e, EstadoAsiento::Reservado(_))).count();
    // Un comprador no puede tener 2 asientos y cada comprador que compró pagó exactamente `precio`.
    let mut unicos = vendidos.clone();
    unicos.sort();
    unicos.dedup();
    let pagos_ok = vendidos
        .iter()
        .all(|u| saldos_iniciales[u] - saldos_finales[u] == precio);
    ResumenVentas {
        vendidos: vendidos.len(),
        cobrado,
        reservas_colgadas: colgadas,
        exitos: resultados.iter().filter(|r| **r == ResultadoCompra::Exito).count(),
        sin_asiento: resultados.iter().filter(|r| **r == ResultadoCompra::AsientoNoDisponible).count(),
        sin_saldo: resultados.iter().filter(|r| **r == ResultadoCompra::SaldoInsuficiente).count(),
        consistente: unicos.len() == vendidos.len()
            && cobrado == vendidos.len() as u64 * precio
            && colgadas == 0
            && pagos_ok,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debitos_concurrentes_no_sobregiran() {
        // 50 hilos piden 30 sobre un saldo de 1000: solo 33 entran (990) y sobran 10.
        let (ok, saldo) = simular_debitos_concurrentes(1000, 30, 50);
        assert_eq!(ok, 33);
        assert_eq!(saldo, 10);
    }

    #[test]
    fn nunca_se_vende_dos_veces_ni_se_cobra_de_mas() {
        for _ in 0..20 {
            let r = simular_ventas(20, 100, 100, 50);
            assert!(r.consistente, "{:?}", r);
            assert!(r.vendidos <= 20);
            // Hay muchos más compradores con saldo que asientos: se vende al menos uno.
            // (No se exige "== 20": una reserva transitoria de un comprador sin saldo
            // puede hacer que otro desista; lo que importa es la consistencia.)
            assert!(r.vendidos > 0);
        }
    }

    #[test]
    fn saldo_insuficiente_libera_la_reserva() {
        // 1 asiento, 1 comprador pobre: queda libre y sin cobro.
        let (inv, h_inv) = InventarioEntradas::iniciar(1);
        let (cuenta, h_c) = CuentaUsuario::iniciar(1, 10);
        assert_eq!(comprar(1, 1, 50, &cuenta, &inv), ResultadoCompra::SaldoInsuficiente);
        inv.send(MsgInventario::Detener).unwrap();
        cuenta.send(MsgCuenta::Detener).unwrap();
        assert_eq!(h_inv.join().unwrap()[&1], EstadoAsiento::Libre);
        assert_eq!(h_c.join().unwrap(), 10);
    }
}
