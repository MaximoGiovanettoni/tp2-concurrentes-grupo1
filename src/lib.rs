//! TP N°2 — Motor Distribuido de Reservas y Pagos.
//!
//! Módulos (uno por parte del enunciado):
//! - `actores`   : Parte 1, modelo de actores con `std::sync::mpsc`.
//! - `heartbeat` : Parte 2.1, detector de fallos con `recv_timeout`.
//! - `bully`     : Parte 2.2, algoritmo de elección Bully (simulado con mensajes).
//! - `dos_fases` : Parte 3, coordinador y participantes de Two-Phase Commit.
//! - `petri`     : Parte 4, simulador de Redes de Petri para verificar el 2PC.

pub mod actores;
pub mod bully;
pub mod dos_fases;
pub mod heartbeat;
pub mod petri;
