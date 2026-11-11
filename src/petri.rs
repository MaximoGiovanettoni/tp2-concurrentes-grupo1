//! PARTE 4 — Redes de Petri: modelo del 2PC (Coordinador + 1 Participante) y verificación.
//!
//! Se implementa un pequeño simulador de redes Lugar/Transición con arcos de peso 1:
//!   - `explorar()` calcula el espacio de estados completo (grafo de alcanzabilidad),
//!   - de ahí salen: acotamiento (máximo de tokens por lugar), marcados muertos (deadlocks)
//!     y la verificación de propiedades de seguridad,
//!   - `es_invariante()` comprueba un invariante de lugar (vector de pesos y con Cᵀ·y = 0).

use std::collections::{HashSet, VecDeque};

pub type Marcado = Vec<u32>;

pub struct Transicion {
    pub nombre: &'static str,
    pub pre: Vec<usize>,
    pub post: Vec<usize>,
}

pub struct RedPetri {
    pub lugares: Vec<&'static str>,
    pub transiciones: Vec<Transicion>,
    pub inicial: Marcado,
}

pub struct Analisis {
    pub alcanzables: Vec<Marcado>,
    pub max_tokens: Vec<u32>,
    pub muertos: Vec<Marcado>,
    pub truncado: bool,
}

const LIMITE_ESTADOS: usize = 100_000;

impl RedPetri {
    pub fn nueva(lugares: &[&'static str], con_token: &[&str]) -> Self {
        let mut r = RedPetri { lugares: lugares.to_vec(), transiciones: vec![], inicial: vec![0; lugares.len()] };
        for l in con_token {
            let i = r.lugar(l);
            r.inicial[i] = 1;
        }
        r
    }

    pub fn lugar(&self, nombre: &str) -> usize {
        self.lugares.iter().position(|l| *l == nombre).unwrap_or_else(|| panic!("lugar inexistente: {}", nombre))
    }

    pub fn transicion(&mut self, nombre: &'static str, pre: &[&str], post: &[&str]) {
        let pre = pre.iter().map(|l| self.lugar(l)).collect();
        let post = post.iter().map(|l| self.lugar(l)).collect();
        self.transiciones.push(Transicion { nombre, pre, post });
    }

    pub fn tokens(&self, m: &Marcado, lugar: &str) -> u32 {
        m[self.lugar(lugar)]
    }

    pub fn habilitada(&self, m: &Marcado, t: usize) -> bool {
        self.transiciones[t].pre.iter().all(|&p| m[p] >= 1)
    }

    pub fn disparar(&self, m: &Marcado, t: usize) -> Marcado {
        let mut n = m.clone();
        for &p in &self.transiciones[t].pre {
            n[p] -= 1;
        }
        for &p in &self.transiciones[t].post {
            n[p] += 1;
        }
        n
    }

    /// BFS sobre el grafo de alcanzabilidad.
    pub fn explorar(&self) -> Analisis {
        let mut vistos: HashSet<Marcado> = HashSet::new();
        let mut cola = VecDeque::new();
        let mut alcanzables = vec![];
        let mut muertos = vec![];
        let mut max_tokens = vec![0; self.lugares.len()];
        let mut truncado = false;
        vistos.insert(self.inicial.clone());
        cola.push_back(self.inicial.clone());
        while let Some(m) = cola.pop_front() {
            for (i, &t) in m.iter().enumerate() {
                max_tokens[i] = max_tokens[i].max(t);
            }
            let hab: Vec<usize> = (0..self.transiciones.len()).filter(|&t| self.habilitada(&m, t)).collect();
            if hab.is_empty() {
                muertos.push(m.clone());
            }
            for t in hab {
                let n = self.disparar(&m, t);
                if !vistos.contains(&n) {
                    if vistos.len() >= LIMITE_ESTADOS {
                        truncado = true;
                        continue;
                    }
                    vistos.insert(n.clone());
                    cola.push_back(n);
                }
            }
            alcanzables.push(m);
        }
        Analisis { alcanzables, max_tokens, muertos, truncado }
    }

    /// Invariante de lugar: para toda transición, la suma ponderada de tokens que consume es igual
    /// a la que produce (es decir, Cᵀ·y = 0). Entonces y·M es constante en todo marcado alcanzable.
    pub fn es_invariante(&self, pesos: &[(&str, i64)]) -> bool {
        let mut y = vec![0i64; self.lugares.len()];
        for (l, w) in pesos {
            y[self.lugar(l)] = *w;
        }
        self.transiciones.iter().all(|t| {
            let consume: i64 = t.pre.iter().map(|&p| y[p]).sum();
            let produce: i64 = t.post.iter().map(|&p| y[p]).sum();
            consume == produce
        })
    }

    pub fn valor_invariante(&self, pesos: &[(&str, i64)], m: &Marcado) -> i64 {
        pesos.iter().map(|(l, w)| *w * m[self.lugar(l)] as i64).sum()
    }

    /// El participante terminó (Commit o Rollback).
    pub fn participante_termino(&self, m: &Marcado) -> bool {
        self.tokens(m, "Estado_Commit") + self.tokens(m, "Estado_Rollback") == 1
    }

    /// Marcado muerto en el que el participante quedó sin decidir = BLOQUEO.
    pub fn bloqueados<'a>(&self, a: &'a Analisis) -> Vec<&'a Marcado> {
        a.muertos.iter().filter(|m| !self.participante_termino(m)).collect()
    }

    pub fn describir(&self, m: &Marcado) -> String {
        let v: Vec<String> = self
            .lugares
            .iter()
            .enumerate()
            .filter(|(i, _)| m[*i] > 0)
            .map(|(i, l)| if m[i] == 1 { l.to_string() } else { format!("{}({})", l, m[i]) })
            .collect();
        format!("{{{}}}", v.join(", "))
    }
}

pub const LUGARES: [&str; 14] = [
    "Coordinador_Inicio",
    "Coordinador_Esperando_Votos",
    "Coordinador_Commit",
    "Coordinador_Rollback",
    "Coordinador_Caido",
    "Participante_Init",
    "Participante_Prepared",
    "Estado_Commit",
    "Estado_Rollback",
    "Msg_Prepare",
    "Msg_VoteYes",
    "Msg_VoteNo",
    "Msg_Commit",
    "Msg_Rollback",
];

/// Invariante del participante: exactamente 1 token entre sus 4 estados.
pub const INV_PARTICIPANTE: [(&str, i64); 4] =
    [("Participante_Init", 1), ("Participante_Prepared", 1), ("Estado_Commit", 1), ("Estado_Rollback", 1)];

/// Invariante del coordinador: exactamente 1 token entre sus estados.
pub const INV_COORDINADOR: [(&str, i64); 5] = [
    ("Coordinador_Inicio", 1),
    ("Coordinador_Esperando_Votos", 1),
    ("Coordinador_Commit", 1),
    ("Coordinador_Rollback", 1),
    ("Coordinador_Caido", 1),
];

/// Red del 2PC con 1 participante.
/// `con_fallos = false`: protocolo básico (camino exitoso + voto NO).
/// `con_fallos = true`: agrega timeout de votos, rollback de un participante que no llegó a votar
/// y la caída del coordinador mientras espera votos (lo que en el código es `CaeTrasPrepare`).
pub fn modelo_2pc(con_fallos: bool) -> RedPetri {
    let mut r = RedPetri::nueva(&LUGARES, &["Coordinador_Inicio", "Participante_Init"]);
    r.transicion("t1_EnviarPrepare", &["Coordinador_Inicio"], &["Coordinador_Esperando_Votos", "Msg_Prepare"]);
    r.transicion("t2_VotaYes", &["Participante_Init", "Msg_Prepare"], &["Participante_Prepared", "Msg_VoteYes"]);
    r.transicion("t3_VotaNo", &["Participante_Init", "Msg_Prepare"], &["Estado_Rollback", "Msg_VoteNo"]);
    r.transicion("t4_DecideCommit", &["Coordinador_Esperando_Votos", "Msg_VoteYes"], &["Coordinador_Commit", "Msg_Commit"]);
    r.transicion("t5_DecideRollback", &["Coordinador_Esperando_Votos", "Msg_VoteNo"], &["Coordinador_Rollback", "Msg_Rollback"]);
    r.transicion("t6_AplicaCommit", &["Participante_Prepared", "Msg_Commit"], &["Estado_Commit"]);
    r.transicion("t7_AplicaRollback", &["Participante_Prepared", "Msg_Rollback"], &["Estado_Rollback"]);
    if con_fallos {
        r.transicion("t8_TimeoutVotos", &["Coordinador_Esperando_Votos"], &["Coordinador_Rollback", "Msg_Rollback"]);
        r.transicion("t9_RollbackSinVotar", &["Participante_Init", "Msg_Rollback"], &["Estado_Rollback"]);
        r.transicion("t10_CaeCoordinador", &["Coordinador_Esperando_Votos"], &["Coordinador_Caido"]);
    }
    r
}

/// Variante que corrige el bloqueo: al reiniciarse, el coordinador (que persistió en su log los
/// votos recibidos) retoma la decisión. Es el modelo de "coordinador con recuperación".
pub fn modelo_con_recuperacion() -> RedPetri {
    let mut r = modelo_2pc(true);
    r.transicion("t11_RecuperaCommit", &["Coordinador_Caido", "Msg_VoteYes"], &["Coordinador_Commit", "Msg_Commit"]);
    r.transicion("t12_RecuperaRollback", &["Coordinador_Caido", "Msg_VoteNo"], &["Coordinador_Rollback", "Msg_Rollback"]);
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sin_commit_y_rollback_a_la_vez(r: &RedPetri, a: &Analisis) -> bool {
        a.alcanzables.iter().all(|m| {
            r.tokens(m, "Estado_Commit") + r.tokens(m, "Estado_Rollback") <= 1
                && !(r.tokens(m, "Coordinador_Commit") == 1 && r.tokens(m, "Estado_Rollback") == 1)
                && !(r.tokens(m, "Coordinador_Rollback") == 1 && r.tokens(m, "Estado_Commit") == 1)
        })
    }

    #[test]
    fn modelo_basico_es_seguro_sin_bloqueos_y_con_invariantes() {
        let r = modelo_2pc(false);
        let a = r.explorar();
        assert!(!a.truncado);
        assert!(a.max_tokens.iter().all(|&t| t <= 1), "debe ser 1-acotada: {:?}", a.max_tokens);
        assert!(r.es_invariante(&INV_PARTICIPANTE));
        assert!(r.es_invariante(&INV_COORDINADOR));
        assert_eq!(r.valor_invariante(&INV_PARTICIPANTE, &r.inicial), 1);
        assert!(a.alcanzables.iter().all(|m| r.valor_invariante(&INV_PARTICIPANTE, m) == 1));
        assert!(sin_commit_y_rollback_a_la_vez(&r, &a));
        assert!(r.bloqueados(&a).is_empty(), "sin fallas no hay bloqueo");
        // El camino exitoso existe: algún marcado alcanzable termina en Commit.
        assert!(a.alcanzables.iter().any(|m| r.tokens(m, "Estado_Commit") == 1 && r.tokens(m, "Coordinador_Commit") == 1));
    }

    #[test]
    fn modelo_con_fallos_sigue_siendo_seguro_pero_aparece_el_bloqueo() {
        let r = modelo_2pc(true);
        let a = r.explorar();
        assert!(a.max_tokens.iter().all(|&t| t <= 1));
        assert!(r.es_invariante(&INV_PARTICIPANTE));
        assert!(r.es_invariante(&INV_COORDINADOR));
        assert!(sin_commit_y_rollback_a_la_vez(&r, &a), "SAFETY se mantiene");
        let bloq = r.bloqueados(&a);
        assert!(!bloq.is_empty(), "LIVENESS se pierde: hay marcados muertos sin decisión");
        // Todos los bloqueos: coordinador caído y participante PREPARED.
        assert!(bloq.iter().all(|m| r.tokens(m, "Coordinador_Caido") == 1 && r.tokens(m, "Participante_Prepared") == 1));
    }

    #[test]
    fn sin_la_transicion_de_caida_no_hay_bloqueo_aun_con_timeouts() {
        // Quitamos t10: el timeout del coordinador (t8) alcanza para que todo termine.
        let mut r = modelo_2pc(true);
        r.transiciones.retain(|t| t.nombre != "t10_CaeCoordinador");
        let a = r.explorar();
        assert!(r.bloqueados(&a).is_empty());
    }

    #[test]
    fn con_recuperacion_del_coordinador_desaparece_el_bloqueo() {
        let r = modelo_con_recuperacion();
        let a = r.explorar();
        assert!(a.max_tokens.iter().all(|&t| t <= 1));
        assert!(r.es_invariante(&INV_PARTICIPANTE));
        assert!(sin_commit_y_rollback_a_la_vez(&r, &a));
        assert!(r.bloqueados(&a).is_empty(), "ya no hay marcados muertos sin decisión");
    }
}
