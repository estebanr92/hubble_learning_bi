# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (DABs) for deployment. Add project-specific instructions below.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill.**

It sets you up to work with this project reliably: CLI authentication, profile
selection, data discovery, and the bundle deployment workflow. Without it,
results are often slower and less accurate.

If this skill is not available (Databricks AI Tools are not installed), you can install them for your coding agent in seconds:

```bash
databricks aitools install
```

If the CLI is not installed, see: https://docs.databricks.com/dev-tools/cli/install

---

## Project Instructions

### 1. Contexto general (qué es esto y para qué)

KindScreen es una app móvil que enseña habilidades socioemocionales a niños. Al registrarse,
el padre/madre llena una encuesta sobre su hijo/a (respuestas: sí / no / a veces).

Este proyecto (`hubble_learning_bi`) es un **POC de portfolio**: tomar esa encuesta como
fuente de datos y construir, de punta a punta, un pipeline de datos en Databricks
(Bronze → Silver → Gold, probablemente con Lakeflow Declarative Pipelines / DLT — el scaffold
ya trae `databricks-dlt` como dependencia en `pyproject.toml`) que termine en un dashboard BI.
El objetivo no es correr esto en producción para KindScreen — es demostrar la construcción
completa de un pipeline de datos profesional (ingesta incremental, modelado, calidad de datos,
privacidad, agregaciones, dashboard) usando un dominio real y con sentido de negocio.

Todo el diseño y la generación de datos mock se hizo en otro repo (`kindscreen-mobile`, el
código fuente de la app, en `~/Documents/Aplicaciones/kindscreen-mobile`). Ese repo **no es
necesario** para trabajar acá — esta sección ya trae todo lo relevante. El detalle original
completo, si hace falta, está en `data-pipeline-claude.md` y `data-pipeline-overview.md` en la
raíz de ese otro repo.

### 2. Qué se hizo hasta ahora (fuera de este repo)

1. Se partió de una encuesta conceptual de 76 preguntas de comportamiento socioemocional
   (existía en el código de la app pero no se usaba en producción — no importa para este
   proyecto, es solo el origen conceptual).
2. Se decidió, como parte del diseño del POC, que el cliente **redujo la encuesta a 30
   preguntas** (menos fricción en el onboarding). Todo el pipeline está construido sobre esas
   30 preguntas.
3. Se categorizaron las 30 preguntas en 9 categorías tipo SDQ (ver sección 3) y se les asignó
   polaridad (positiva/negativa) para poder calcular un score.
4. Se definieron reglas de privacidad estrictas (ver sección 4) — dato sensible: niños.
5. Se escribió un generador en Python (`generate_mock_data.py`, solo stdlib) que simula **6
   meses de exportación diaria** (2024-02-26 a 2024-08-25) de un proceso manual que después se
   descontinuó, y **ya se ejecutó**: generó 547 archivos CSV (~12 MB) con 2,899 niños, 81,611
   respuestas de encuesta y 12,750 filas de prioridades de habilidades.
6. Esos CSVs **todavía no están en Databricks**. Viven localmente en:
   `~/Documents/Aplicaciones/kindscreen-mobile/data-pipeline-mock/raw_landing/`
   Hay que traerlos a este proyecto — copiarlos a `fixtures/` (que ya existe, vacía, parece
   pensada para esto) y/o subirlos a un Volumen de Unity Catalog. Ese es el primer paso
   pendiente.

**Lo que falta y es el objetivo de este proyecto**: todo lo de Databricks en sí — ingesta a
Bronze, transformaciones a Silver y Gold, y el dashboard. Nada de eso existe todavía
(`resources/` y `src/` están vacíos).

### 3. El modelo de datos

#### 3.1 Las 30 preguntas y sus categorías

| Categoría | Descripción | # preguntas |
|---|---|---|
| `parent_confidence` | Confianza del padre hablando de emociones y comportamiento con su hijo/a | 3 |
| `emotional_symptoms` | Ansiedad, tristeza, miedos (internalizing) | 4 |
| `conduct_problems` | Pierde temperamento, pelea/molesta (externalizing) | 2 |
| `hyperactivity_inattention` | Atención, distracción | 2 |
| `peer_relationships` | Amistades, ser molestado/bullying | 3 |
| `prosocial_behavior` | Ayudar, compartir, ser amable, empatía | 4 |
| `communication` | Conversación, cortesía, contacto visual | 5 |
| `self_regulation` | Calma ante provocación, seguir reglas, aceptar críticas | 6 |
| `responsibility` | Asumir errores | 1 |

Cada pregunta tiene `polarity` (`positive`/`negative`). Las "negative" están redactadas como
dificultad (ej. "se preocupa mucho") y su score se invierte antes de agregar:

```
raw_value: no=0, a_veces=1, si=2
normalized_value = raw_value            si polarity = positive
normalized_value = 2 - raw_value        si polarity = negative
```

El mapeo completo de las 30 preguntas (id, texto EN/ES, categoría, polaridad) está en el CSV
`dim_questions.csv` que ya viene generado (ver sección 2, punto 6) — es el catálogo fuente de
verdad, no hace falta retranscribirlo acá.

#### 3.2 Los 4 archivos CSV de origen (landing zone)

Simulan un export diario incremental (tipo CDC) de un backend de la app. Ya están generados
para 182 días (2024-02-26 a 2024-08-25), organizados en carpetas `dt=YYYY-MM-DD/`:

- **`dim_questions.csv`** (estático, una sola vez): `question_id, text_en, text_es, category,
  polarity`
- **`child_account_updates.csv`** (uno por día): `child_id, child_gender, birth_date,
  relationship_to_child, region, locale, created_at`
- **`survey_responses.csv`** (uno por día, long format — una fila por pregunta respondida):
  `response_row_id, response_id, child_id, question_id, answer_value, locale, submitted_at,
  app_version, platform`
- **`skill_priorities.csv`** (uno por día, solo si hubo datos ese día): `child_id, rank (1-5),
  skill_id, submitted_at`

Notas de realismo ya incorporadas en los datos (importan para el diseño de Silver):
- ~12% de encuestas están incompletas (niños que respondieron entre 5 y 25 de las 30 preguntas)
- Solo quienes respondieron ≥25 preguntas tienen fila en `skill_priorities` (embudo real)
- ~1.5% de `answer_value` llegan "sucios" (`"Si"`, `"NO "`, `"A_Veces"` en vez de
  `si`/`no`/`a_veces`) y ~0.3% de filas de `survey_responses` están duplicadas — a propósito,
  para justificar reglas de limpieza reales en Silver
- Existe un drift temporal real: el score de `parent_confidence` sube de ~41% "sí" en febrero a
  ~70% en agosto — útil para las vistas de tendencia por cohorte
- La encuesta se llena **una única vez por niño**, al registrarse — no hay reevaluaciones ni
  seguimiento longitudinal del mismo niño en el tiempo. Las tendencias del dashboard comparan
  **cohortes de fecha de registro** entre sí, nunca al mismo niño contra sí mismo.

### 4. Reglas de privacidad (no negociables — datos de niños)

Esto gobierna todo el diseño de Silver y Gold, y cualquier código que se escriba en este
proyecto debe respetarlo:

- **Nunca hay grano de niño individual en Gold ni en el dashboard.** No existe (ni debe
  existir) una vista tipo "perfil de un niño específico". Todo lo publicado son agregados de
  un grupo (cohorte, región, género, etc.).
- **`child_id` es una llave pseudónima de uso interno**, necesaria en Bronze/Silver para unir
  correctamente las respuestas de un mismo niño y calcular bien su score antes de agregar.
  **Ninguna tabla Gold debe conservar `child_id`** — se usa y se descarta en la transición
  Silver → Gold.
- **No hay nombre ni ningún dato identificable** en los datos de origen — ya se diseñó así
  desde el generador, no hay que anonimizar nada porque nunca se capturó.
- **`birth_date` exacto solo debe vivir en Bronze.** Desde Silver en adelante, derivar y usar
  `age_band` (ej. `"4-6"`, `"7-9"`, `"10-12"`), nunca la fecha exacta ni la edad exacta.
- **Regla de tamaño mínimo de grupo (k-anonimato simplificado): ninguna fila de una tabla Gold
  segmentada debe representar a menos de 20 niños.** Combinaciones muy específicas (ej.
  "abuela, región UK, marzo") con pocos casos deben agruparse en un bucket
  `other`/`insufficient_data` en vez de publicarse tal cual. Hay que implementarlo como parte
  de las queries/transformaciones de Gold, no es solo una nota de diseño.
- `child_gender` (`female`/`male`/`prefer_not_to_say`) sí es un dato demográfico legítimo para
  agregar/comparar grupos — a diferencia de un nombre o ID, no identifica a nadie por sí solo.

### 5. Arquitectura objetivo (medallion)

**La historia del landing (decidida)**: el backstory ficticio es que un Cloud Function (GCP,
coherente con que KindScreen usa Firebase/Firestore) leía Firestore y dejaba los CSV diarios en
un bucket de GCS. Evaluamos usar Auto Loader cross-cloud (el workspace de Databricks corre en
AWS) para leer ese bucket directo — es técnicamente posible (Auto Loader solo necesita
credenciales de una service account de GCP, no requiere que el workspace esté en GCP), pero
decidimos que **no es la historia correcta**: este fue un piloto de 6 meses hecho por un equipo
chico y después descontinuado — no tendría sentido que hubieran invertido en una integración
cross-cloud automatizada (con su costo de egress y complejidad operativa) para algo
experimental. La historia real y consistente es: **el CSV se bajaba de GCS a mano y se subía a
mano a un Volumen de Unity Catalog** — por eso en este proyecto no hay Auto Loader apuntando a
`gs://` ni a `s3://`, todo entra por el Volumen.

El Volumen ya existe y se llama **`raw`** (dentro del catálogo/esquema de la sección 6). Los
CSVs se subieron con su contenido directo bajo la raíz del volumen (sin wrapper
`raw_landing/`), o sea la ruta real es `/Volumes/hubble_prod/<schema>/raw/dt=YYYY-MM-DD/...` y
`/Volumes/hubble_prod/<schema>/raw/dim_questions.csv`.

```
Volumen UC (landing)          Bronze                  Silver                      Gold
raw/                           raw_survey_responses  →  fact_survey_answers    →   agg_scores_by_cohort
  dt=YYYY-MM-DD/                raw_child_updates        dim_child (interno)       agg_scores_by_segment
                                 raw_skill_priorities     dim_question              gap_analysis_summary
                                                           dim_skill                 funnel_completion
                                                           dim_date                   dq_pipeline_observability
```

**Bronze** (landing → Bronze): Auto Loader (`cloudFiles`) o `COPY INTO` leyendo el volumen
(particionado por `dt=YYYY-MM-DD/`), columnas de auditoría (`_ingested_at`, `_source_file`),
schema inference con `rescuedDataColumn`.

**Silver** (Bronze → Silver): normalizar `answer_value` (limpiar los valores "sucios"),
aplicar polaridad (join con `dim_questions`), dedupe por `response_row_id`, validar catálogo
de valores permitidos, derivar `dim_date` y `age_band`. `fact_survey_answers` y `dim_child`
**sí** tienen grano de niño porque hace falta para promediar bien — pero son tablas internas,
nunca expuestas fuera de Silver.

**Gold** (Silver → Gold): agregaciones finales, sin `child_id`, con umbral mínimo de grupo
(≥20). Tablas objetivo:

| Tabla Gold | Grano | Uso |
|---|---|---|
| `agg_scores_by_cohort` | mes de registro × categoría | Tendencia por cohorte de alta |
| `agg_scores_by_segment` | región/locale/relación/género/age_band × categoría (≥20) | Comparativas demográficas |
| `gap_analysis_summary` | mes de registro (o global) | % cuya categoría más débil coincide con su prioridad elegida |
| `funnel_completion` | mes de registro | % que completan encuesta / eligen habilidades |
| `dq_pipeline_observability` | fecha de ingesta | Volumen ingerido, freshness, % filas limpiadas/rechazadas |

**Dashboard (AI/BI Databricks)**: serie de tiempo por categoría/cohorte, bar chart por
categoría, distribución por locale/género/relación, scatter `parent_confidence` vs. resto de
categorías (a nivel de segmento), funnel de conversión, gap analysis, tile de observabilidad
del pipeline. Framing: es un análisis **retrospectivo** de un piloto de 6 meses ya cerrado.

### 6. Catálogo y esquema reales de este proyecto

Definidos en `databricks.yml` (no son placeholders, ya son los reales):
- Catálogo: `hubble_prod` (`${var.catalog}`)
- Esquema: `${workspace.current_user.short_name}` en `dev`, `prod` en `prod` (`${var.schema}`)
- Workspace: `https://dbc-45a56bff-f3aa.cloud.databricks.com`
- Volumen de landing: **`raw`** (ya creado). Ruta completa: `/Volumes/hubble_prod/<schema>/raw/`
  (el `<schema>` depende del target — ver arriba). Ver sección 5 para la historia del landing
  (Cloud Function → GCS como backstory, subida manual al Volumen en la práctica).

### 7. Funciones que debe cumplir Claude en este proyecto

- **Armar y mantener la estructura del DAB** (`resources/*.yml`, jobs/pipelines, targets
  dev/prod ya definidos en `databricks.yml`) siguiendo las convenciones estándar de Databricks
  Asset Bundles.
- **Escribir las transformaciones Bronze → Silver → Gold**, preferentemente con Lakeflow
  Declarative Pipelines / DLT (ya está la dependencia en `pyproject.toml`), aplicando siempre
  las reglas de la sección 4 sin excepción.
- **Configurar la ingesta** desde el volumen (Auto Loader/COPY INTO) respetando la partición
  `dt=YYYY-MM-DD/` ya existente en los CSVs.
- **Implementar y verificar la regla de tamaño mínimo de grupo (≥20)** en cualquier query o
  transformación que produzca una tabla Gold segmentada — no asumir que ya está aplicada,
  chequearlo explícitamente en cada tabla nueva.
- **Ayudar a construir el AI/BI Dashboard** (queries SQL sobre Gold, definición de visuales).
- **Mantener esta sección actualizada** como fuente de verdad del proyecto a medida que se
  tomen decisiones nuevas (nombre real del volumen, cambios de diseño, etc.) — así una sesión
  futura no depende de releer todo el historial de conversación.
- **Preguntar antes de acciones irreversibles o que afecten estado compartido** (crear/borrar
  catálogos, esquemas o volúmenes reales, correr jobs que escriban sobre datos ya cargados,
  `databricks bundle deploy` a `prod`) — mismo criterio de cautela que en cualquier otro
  proyecto.
- **No inventar detalles del negocio o de la app KindScreen que no estén en este documento** —
  todo lo relevante de la app/encuesta ya está resumido acá; si hace falta algo que no está,
  preguntar en vez de asumir.

### 8. Estado actual / próximo paso inmediato

- [x] Diseño del modelo de datos, categorización, privacidad
- [x] 6 meses de CSVs mock generados (fuera de este proyecto, ver sección 2)
- [x] Proyecto DAB creado (`hubble_learning_bi`)
- [x] Volumen de landing creado (`raw`) y decidida la historia del landing (ver sección 5)
- [ ] Confirmar que la subida de los 547 CSVs al volumen `raw` ya se completó (última acción en
      curso al momento de escribir esto — verificar con `databricks fs ls` antes de asumir)
- [ ] Notebooks/pipelines de Bronze → Silver → Gold
- [ ] Dashboard AI/BI
