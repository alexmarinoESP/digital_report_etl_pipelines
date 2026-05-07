# Shared Code & Cross-Platform Pitfalls

**Last Updated**: 2026-05-07

Questo documento descrive le utility condivise tra le pipeline social
(`linkedin`, `google`, `facebook`, …), i rischi noti di regressione
cross-platform, le safety net già in piedi e una checklist da seguire
**prima** di toccare codice sotto `social/utils/` o `social/infrastructure/`.

> **Perché esiste**: il 2026-05-07 una modifica voluta solo per LinkedIn ha
> rotto Facebook in produzione (vedi [Caso 1](#caso-1-2026-05-07--linkedin-rompe-facebook)).
> La causa è strutturale: piccole modifiche a moduli condivisi cambiano il
> comportamento per tutti i social, anche quando l'autore ne sta toccando
> uno solo.

---

## Indice

1. [Mappa dei moduli condivisi](#mappa-dei-moduli-condivisi)
2. [Pattern di rischio noti](#pattern-di-rischio-noti)
3. [Safety net in piedi](#safety-net-in-piedi)
4. [Checklist pre-push per modifiche shared](#checklist-pre-push-per-modifiche-shared)
5. [Casi storici](#casi-storici)

---

## Mappa dei moduli condivisi

| Modulo | Funzione/Classe | Usato da | Impatto di una modifica |
|---|---|---|---|
| `social/infrastructure/database.py` | `VerticaDataSink.load`, `_copy_to_db`, `_upsert`, `_increment`, `_detect_pk_columns` | **TUTTI i social** | massimo: ogni write a DB passa di qui |
| `social/utils/aggregation.py` | `aggregate_metrics_by_entity` | Facebook (4 tabelle), LinkedIn (3 tabelle) | alto: cambia somma/aggregato di insight |
| `social/utils/commons.py` | `handle_nested_response` | LinkedIn + chiunque flatti JSON nested | medio |
| `social/utils/commons.py` | `extract_targeting_criteria` | LinkedIn | basso (LinkedIn-only) |
| `social/core/constants.py` | `DATABASE_TEST_SUFFIX`, `PIPE_DELIMITER`, `ESCAPE_CHARS` | TUTTI | medio |
| `social/core/protocols.py` | `TokenProvider`, `DataSink` | TUTTI | alto se cambia il contratto |

Per il dettaglio aggiornato di chi importa cosa, basta:

```bash
grep -rn "from social.utils.aggregation import" social/
grep -rn "from social.utils.commons import"     social/
```

---

## Pattern di rischio noti

### 1. SDK che restituiscono numeri come stringhe

`facebook-business` restituisce le metriche come stringhe (`"56.77"`,
`"14455"`). Codice che si aspetta dtype numerico fallisce silenziosamente:

```python
numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
# -> []  perché il dtype è 'string'/'object'
```

**Mitigazione**: `aggregate_metrics_by_entity` ora forza `pd.to_numeric` con
`errors='coerce'` su tutte le candidate metric column **prima** del check
dtype, gestendo sia `object` che il `string` di pandas 2.x.

Quando si introduce una nuova shared utility che fa math, **assumere che
ogni colonna numerica possa arrivare come stringa**.

### 2. Sentinel pandas `INT64_MIN` su BIGINT Vertica

Quando una colonna `int64` contiene NaN, pandas la materializza come
`-9223372036854775808` (= `INT64_MIN`). Quando va al `COPY FROM STDIN`,
viene serializzata come testo letterale. Vertica BIGINT considera quel
valore come "internal NULL marker" e rifiuta il record con:

```
COPY: Input record N has been rejected
(int8 out of range '-9223372036854775808' for column X)
```

**Da dove arriva il NaN su int64**:
- `pd.to_numeric('', errors='coerce')` → NaN
- `groupby(...).sum()` su un gruppo all-NaN
- Concatenazione fra dataframe con e senza la colonna
- Cast manuali a `int64` di colonne nullable

**Mitigazioni in piedi**:
1. `aggregate_metrics_by_entity` fa `fillna(0)` sulle metriche dopo la sum
   (semantica: una metrica additiva mancante = 0).
2. `_copy_to_db` ha una **safety net universale** che intercetta i sentinel
   in tutte le colonne `int64`/`Int64` e li sostituisce con `None` prima
   del COPY, loggando un warning. Funziona per qualunque social, presente o
   futuro.

### 3. PK auto-detect che maschera bug

`_detect_pk_columns` ha più regole con `return` immediato. La prima che
matcha vince, anche se non è quella giusta:

- Rule 1: trova `id` → ritorna `['id']` (anche se la PK reale è composta)
- Fallback finale: ritorna **tutte** le colonne non-metadata come PK

Effetto: tabelle senza `pk_columns` esplicito nel YAML possono finire con
una PK ampia che "filtra" i NaN nelle metriche via
`dropna(subset=pk_columns)`, mascherando bug a monte.
Quando si fixa la PK (rendendola stretta e corretta), i bug latenti
emergono.

**Linea guida**: ogni tabella che usa upsert/append/replace deve avere
`pk_columns`/`dedupe_columns` esplicito nel YAML. **Non affidarsi mai
all'auto-detect** in produzione.

### 4. `truncate: True` ignora `append.pk_columns`

Il pipeline di Google e Facebook leggono il `load_mode` da una catena di
`if/elif` sul YAML. Se il `truncate: True` è valutato prima di `append`,
una sezione `append.pk_columns` è morta: il load mode è `replace` e
`pk_columns` resta `None`. Stesso vale per `upsert`/`increment`
combinati.

**Linea guida**: per i `replace` (cioè quando hai `truncate: True` nel
YAML) dichiara `dedupe_columns` al **top-level**, NON sotto `append:`.
Il pipeline lo legge correttamente. Esempio corretto:

```yaml
google_ads_audience:
  truncate: true
  dedupe_columns:           # top-level
    - id
    - display_name
```

### 5. Modifiche a chunking che cambiano numero di righe per entità

Pipeline che chunkano per data range (es. `fb_ads_insight` con
`date_preset: maximum`) restituiscono N righe per `ad_id` (una per chunk).
Aspettano un `aggregate_by_entity` a valle che le sommi. Se l'aggregate
fallisce o non aggrega davvero, il `drop_duplicates(['ad_id'])` successivo
butta N-1 chunk e tiene solo il primo/ultimo.

Sintomo: numeri in DB che sono ~1/N rispetto a Meta Business (per
Facebook), ~1/N rispetto a LinkedIn Campaign Manager, ecc.

**Linea guida**: ogni pipeline che chunka deve avere uno step
`aggregate_by_entity` con `metric_columns` espliciti **e** un test che
verifichi che la somma su 2+ chunk produca i totali attesi.

### 6. Ratio (CTR/CPC/CPM) sommati invece che ricalcolati

Sommare ratio è matematicamente sbagliato (`1% + 1% ≠ 2%`). Quando si
aggregano chunk multipli, i ratio vanno **ricalcolati** dai totali sommati:

```
ctr = clicks / impressions * 100
cpc = spend / clicks
cpm = spend / impressions * 1000
```

Per Facebook esiste già `FacebookProcessor.recalculate_ratios`. Se aggiungi
una nuova platform che chunka, replica il pattern.

### 7. Anti-fraud Meta su user-token da nuovi IP

In locale, l'`access_token` user-bound nel `.env` può rispondere
`OAuthException code 200 - Cannot call API for app X on behalf of user Y`
anche se in PROD lo stesso identico token funziona. È un anti-fraud
location-based di Meta, non un bug nostro.

**Linea guida**: in produzione usare un **System User Access Token** dal
Business Manager (long-lived, non user-bound, non subisce anti-fraud per
location). I token user vanno bene solo per dev/test su un IP storicamente
loggato.

---

## Safety net in piedi

| Dove | Cosa fa | Quando scatta | Log |
|---|---|---|---|
| `aggregate_metrics_by_entity` | coerce string→numeric per le metric_columns prima del groupby | sempre, anche con `metric_columns` esplicito | `"✓ Aggregated metrics: N rows → M entities"` |
| `aggregate_metrics_by_entity` | `fillna(0)` su metric_columns dopo groupby sum | quando agg_method=='sum' e c'è almeno un gruppo all-NaN | logga "Aggregated metrics" indicando le colonne |
| `_copy_to_db` | replace `INT64_MIN` sentinel con `None` per tutte le colonne int64/Int64 | quando una qualunque colonna ha quel valore literal | `"Replaced N INT64_MIN sentinel value(s) with NULL in column 'X' before COPY"` |
| `_copy_to_db` | `dropna(subset=pk_columns)` | quando una riga ha NULL nelle PK column | `"Filtered N rows with NULL values in PK columns ..."` |

Quando le safety net scattano in PROD, **non ignorare i warning**: sono
spie di un dato sporco a monte (API restituisce null inaspettati, oppure
il processor non sta facendo coerce/fillna). Va in genere indagato.

---

## Checklist pre-push per modifiche shared

Quando il diff include modifiche a `social/utils/*.py`,
`social/infrastructure/*.py` o `social/core/*.py`:

- [ ] **Esegui in TEST mode i tre principali social** (LinkedIn, Google, Facebook). Non solo quello per cui hai scritto il fix. Ognuno scrive in tabelle `_TEST` ed è non distruttivo.
   ```powershell
   $env:TEST_MODE = "true"
   $env:STORAGE_TYPE = "vertica"
   # … le var per ogni piattaforma …
   .venv\Scripts\python.exe social\platforms\linkedin\run_linkedin.py
   .venv\Scripts\python.exe social\platforms\google\run_google.py
   .venv\Scripts\python.exe social\platforms\facebook\run_facebook.py
   ```
- [ ] **Per ogni social, conta righe scritte vs righe estratte API**. Se vedi `Loaded N rows … from M API rows` con `N << M` su una tabella che fa aggregate, qualcosa non torna.
- [ ] **Cerca i warning della safety net `_copy_to_db`** nei log:
   ```
   Replaced N INT64_MIN sentinel value(s) with NULL …
   Filtered N rows with NULL values in PK columns …
   ```
   Sono spie di dati sporchi che la safety sta tappando ma andrebbero indagati.
- [ ] **Per le tabelle insight con chunking**: verifica via SQL che i totali per un `id` noto sommino tutti i chunk. Esempio Facebook:
   ```sql
   SELECT spend, impressions, clicks
   FROM GoogleAnalytics.fb_ads_insight_TEST
   WHERE ad_id = '<ad_id_test>';
   ```
   Confronta con il totale di Meta Business / Campaign Manager / dashboard nativa.
- [ ] **Mai aggiungere un `_copy_to_db` senza passare `pk_columns`** quando il chiamante ha la PK. Vedi le 5 chiamate in `database.py` post-fix del 2026-05-06.
- [ ] **Mai assumere dtype numerico** in shared utility: usa `pd.api.types.is_numeric_dtype(col)` non `dtype == 'object'`.
- [ ] **Mai sommare ratio** (ctr, cpc, cpm, *_rate). Recompute dai totali aggregati.

---

## Casi storici

### Caso 1: 2026-05-07 — LinkedIn rompe Facebook

**Sintomo**: il job schedulato Facebook delle 02:00 va in `Failed` per la
prima volta in 2 settimane. Errore Vertica:
```
COPY: Input record 21 has been rejected
(int8 out of range '-9223372036854775808' for column 3 (impressions))
```

**Causa**: il commit `f7dc114` deployato il giorno prima conteneva la fix
LinkedIn `extract_targeting_criteria` + propagazione `pk_columns` a
`_copy_to_db`. Effetto laterale per `fb_ads_insight_placement`:

- Prima: `_copy_to_db` cadeva nell'auto-detect PK con fallback "all
  columns". Il `dropna(subset=pk_columns)` filtrava anche le righe con NaN
  su `impressions` perché `impressions` faceva parte della PK fittizia.
- Dopo: PK stretta `[campaign_id, publisher_platform]` (corretta) → il
  `dropna` non protegge più dalle metriche NaN → il NaN su `int64` produce
  il sentinel `INT64_MIN` → Vertica BIGINT lo rifiuta.

Il dato API in sé era invariato: i `null` su `impressions` per
`(campaign_id, publisher_platform)` con zero attività su quella piattaforma
arrivavano da sempre. Il bug era latente.

**Fix in due livelli**:
1. `aggregation.py`: coerce string→numeric e `fillna(0)` post-sum.
2. `database.py::_copy_to_db`: safety net che intercetta `INT64_MIN`
   sentinel su qualsiasi colonna int64.

### Caso 2: 2026-05-06 — Bug "1 audience per campagna"

**Sintomo**: tabella `linkedin_ads_campaign_audience` aveva una sola riga
per ogni campagna, anche se una campagna nel `targetingCriteria` di
LinkedIn può avere 15+ audience.

**Cause** (multiple, sovrapposte):
1. `extract_targeting_criteria` prendeva solo il primo URN della prima
   facet (`segment[0]`) invece di iterare su tutti.
2. YAML `linkedin_ads_campaign_audience` aveva `upsert.pk_columns: [id]`
   da solo: anche correggendo il punto 1, l'upsert avrebbe collassato N
   righe in 1.
3. `_copy_to_db` non riceveva `pk_columns` da `_upsert` quindi ricadeva
   sull'auto-detect `['id']` e droppava le N-1 righe in più.

**Tabelle Google con stesso pattern**: `google_ads_audience` aveva
`append.pk_columns: [id, placement]` morto perché ignorato dal branch
`truncate: True`. Stesso 1 audience per ad_group. Fix: `dedupe_columns:
[id, display_name]` al top-level.

**Lezione**: tre fix erano necessari per chiudere il bug perché ogni layer
maschera quello sotto. Sempre seguire il dato dall'API al COPY end-to-end
quando si indaga "perdita di righe".

---

## Quando aggiornare questo documento

- Hai fixato un bug cross-platform o introdotto una nuova safety net →
  aggiungi al log dei "Casi storici" e alla tabella delle safety.
- Hai aggiunto un nuovo social/tabella che chunka i dati → aggiungi alla
  lista di "tabelle insight con chunking" della checklist.
- Hai introdotto una nuova utility in `social/utils/` → aggiungi alla
  mappa moduli condivisi.
