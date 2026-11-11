//! Ejecutable de demostración. Uso:
//!   cargo run --release -- parte1 | parte2 | parte2-lider | parte3 | parte4 | todo
//! Los tests automáticos se corren con `cargo test`.

use std::time::Duration;
use tp2_reservas::actores;
use tp2_reservas::bully;
use tp2_reservas::dos_fases::*;
use tp2_reservas::heartbeat;
use tp2_reservas::petri;

fn parte1() {
    println!("=== PARTE 1: Actores CuentaUsuario e InventarioEntradas ===");
    let (ok, saldo) = actores::simular_debitos_concurrentes(1000, 30, 50);
    println!("Cuenta única, 50 hilos debitan $30 con saldo $1000 -> débitos OK: {} (esperado 33), saldo final: ${} (esperado $10)", ok, saldo);

    let r = actores::simular_ventas(20, 100, 100, 50);
    println!("100 compradores compiten por 20 asientos (precio $50, 1 de cada 3 sin saldo suficiente):");
    println!("  vendidos={} cobrado=${} (= vendidos*50 -> {})", r.vendidos, r.cobrado, r.vendidos as u64 * 50);
    println!("  compras OK={} sin asiento={} sin saldo={} reservas colgadas={}", r.exitos, r.sin_asiento, r.sin_saldo, r.reservas_colgadas);
    println!("  CONSISTENTE (nada vendido 2 veces, cobros exactos): {}", r.consistente);
}

fn parte2(forzar_lider: bool) {
    println!("=== PARTE 2: Heartbeats + Monitor (+ Bully si cae el líder) ===");
    let n = 3;
    let lider = n - 1; // Bully: el líder es el nodo con mayor id
    let victima = if forzar_lider { lider } else { heartbeat::nodo_aleatorio(n) };
    println!("Nodos 0..{} | líder actual: nodo {} | nodo que se va a caer: {}", n - 1, lider, victima);
    let eventos = heartbeat::simular(heartbeat::Config::enunciado(), n, victima, Duration::from_millis(3000), true);
    let cayo = eventos.iter().any(|(_, e)| *e == heartbeat::Evento::Caido(victima));
    if cayo && victima == lider {
        println!("\nEl Monitor declaró CAIDO al líder -> los nodos restantes eligen líder (Bully):");
        let iniciador = (0..n).find(|&i| i != victima).unwrap();
        let r = bully::simular_eleccion(n, &[victima], iniciador, Duration::from_millis(200), true);
        println!("Líder según cada nodo: {:?} | mensajes intercambiados: {}", r.lideres, r.mensajes);
    } else if cayo {
        println!("\nCayó un nodo que NO era líder: no hace falta elección, el líder sigue siendo el nodo {}.", lider);
    }
}

fn parte3() {
    println!("=== PARTE 3: Two-Phase Commit ===");
    let t = Duration::from_millis(300);
    let casos: Vec<(&str, Escenario)> = vec![
        ("A) Todo OK", Escenario::feliz()),
        ("B) Asiento ya vendido (Inventario vota NO)", Escenario { asiento_libre: false, ..Escenario::feliz() }),
        ("C) Saldo insuficiente (Pagos vota NO)", Escenario { saldo: 10, ..Escenario::feliz() }),
        ("D) ServicioPagos no responde (timeout)", Escenario { pagos: Comportamiento::NoResponde, ..Escenario::feliz() }),
        ("E) El coordinador cae tras el Prepare", Escenario { falla: FallaCoordinador::CaeTrasPrepare, ..Escenario::feliz() }),
    ];
    for (titulo, esc) in casos {
        println!("\n--- {} ---", titulo);
        let (res, reportes) = ejecutar_escenario(esc, t);
        for l in &res.log {
            println!("{}", l);
        }
        println!("  DECISIÓN FINAL: {:?}", res.decision);
        for r in reportes {
            println!("  {} -> {:?} | bloqueado={} | {}", r.nombre, r.estado, r.bloqueado, r.resumen);
            for w in &r.wal {
                println!("      WAL: {}", w);
            }
        }
    }
}

fn parte4() {
    println!("=== PARTE 4: Red de Petri del 2PC (1 coordinador + 1 participante) ===");
    for (titulo, con_fallos) in [("Modelo básico (sin fallas)", false), ("Modelo con fallas (timeout + caída del coordinador)", true)] {
        let r = petri::modelo_2pc(con_fallos);
        let a = r.explorar();
        println!("\n--- {} ---", titulo);
        println!("Lugares: {} | Transiciones: {} | Marcados alcanzables: {}", r.lugares.len(), r.transiciones.len(), a.alcanzables.len());
        println!("Máximo de tokens por lugar: {} -> {}", a.max_tokens.iter().max().unwrap(),
            if a.max_tokens.iter().all(|&t| t <= 1) { "1-ACOTADA (Safe)" } else { "NO es 1-acotada" });
        println!("Invariante participante  (Init+Prepared+Commit+Rollback = 1): {}", r.es_invariante(&petri::INV_PARTICIPANTE));
        println!("Invariante coordinador   (Inicio+Esperando+Commit+Rollback+Caido = 1): {}", r.es_invariante(&petri::INV_COORDINADOR));
        let viola = a.alcanzables.iter().filter(|m| r.tokens(m, "Estado_Commit") + r.tokens(m, "Estado_Rollback") > 1).count();
        println!("Marcados con Commit y Rollback simultáneos en el participante: {}", viola);
        println!("Marcados muertos: {} | de ellos BLOQUEADOS (participante sin decidir): {}", a.muertos.len(), r.bloqueados(&a).len());
        for m in r.bloqueados(&a) {
            println!("   BLOQUEO -> {}", r.describir(m));
        }
    }
    let r = petri::modelo_con_recuperacion();
    let a = r.explorar();
    println!("\n--- Modelo con recuperación del coordinador (t11/t12) ---");
    println!("Marcados alcanzables: {} | 1-acotada: {} | BLOQUEADOS: {}", a.alcanzables.len(),
        a.max_tokens.iter().all(|&t| t <= 1), r.bloqueados(&a).len());
}

fn main() {
    let arg = std::env::args().nth(1).unwrap_or_else(|| "todo".to_string());
    match arg.as_str() {
        "parte1" => parte1(),
        "parte2" => parte2(false),
        "parte2-lider" => parte2(true),
        "parte3" => parte3(),
        "parte4" => parte4(),
        _ => {
            parte1();
            println!();
            parte2(true);
            println!();
            parte3();
            println!();
            parte4();
        }
    }
}
