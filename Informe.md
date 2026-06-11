# Informe - Laboratorio 3

## Ejercicio 1: Identificación de las regiones paralelizables

### Inciso A - Diagrama de flujo y grafo de dependencias

A continuación se describe el flujo de la aplicación utilizando Apache Spark, diferenciando los pasos que coordina el `driver` de aquellos que se distribuyen y ejecutan en paralelo en los distintos `workers`.

#### Paso 1: Lectura de suscripciones (Driver)

- **Acción:** Lectura del archivo JSON local mediante funciones de E/S tradicionales.
- **Salida:** `List[Subscription]`

#### Paso 2: Paralelización (Driver -> Workers)

- **Acción/Transformación:** Conversión de la colección estática de suscripciones en un RDD para distribuir la carga de trabajo (`parallelize`).
- **Entrada:** `List[Subscription]`
- **Salida:** `RDD[Subscription]`

#### Paso 3: Descarga y análisis sintáctico de publicaciones (Workers)

- **Transformación:** Descarga del contenido de los feeds a través de la red y análisis sintáctico del JSON de las publicaciones (`map`). Esta etapa es altamente paralelizable.
- **Entrada:** `RDD[Subscription]`
- **Salida:** `RDD[(Boolean, List[Post])]` *(tupla que indica si la descarga del feed fue exitosa y las publicaciones extraídas)*

#### Paso 4: Unificación y filtrado de publicaciones (Workers)

- **Transformación:** Extracción y consolidación de todas las publicaciones provenientes de las distintas listas (`flatMap`), seguida del filtrado de aquellas que se encuentren vacías o no sean válidas (`filter`).
- **Entrada:** `RDD[(Boolean, List[Post])]`
- **Salida:** `RDD[Post]`

#### Paso 5: Extracción y clasificación de entidades (Workers)

- **Transformación:** Análisis del texto combinado de cada publicaci�n mediante el diccionario para identificar entidades nombradas (`flatMap`). El diccionario debe propagarse utilizando, por ejemplo, variables de tipo *Broadcast*.
- **Entrada:** `RDD[Post]`
- **Salida:** `RDD[NamedEntity]`

#### Paso 6: Asignación para conteo (Workers)

- **Transformación:** Asignación de cada entidad identificada a un par clave-valor, utilizando el tipo y nombre de la entidad como clave, con un valor inicial de 1 para su posterior sumatoria (`map`).
- **Entrada:** `RDD[NamedEntity]`
- **Salida:** `RDD[((String, String), Int)]`

#### Paso 7: Agrupación y conteo (Workers)

- **Transformación:** Sumatoria de las ocurrencias de las entidades que comparten una misma clave, realizada de forma distribuida (`reduceByKey`).
- **Entrada:** `RDD[((String, String), Int)]`
- **Salida:** `RDD[((String, String), Int)]`

#### Paso 8: Clasificación y recolección (Driver)

- **Acción:** Extracción de los resultados y transferencia al programa principal, ordenándolos de forma descendente para obtener las entidades más frecuentes (`takeOrdered` o mediante un `sortBy` seguido de un `take`).
- **Entrada:** `RDD[((String, String), Int)]`
- **Salida:** `Array[((String, String), Int)]`

#### Resumen gráfico del flujo

```text
[Driver]   List[Subscription]
                  | (parallelize)
                  v
[Workers]  RDD[Subscription]
                  | (map: descarga de red y JSON)
                  v
[Workers]  RDD[(Boolean, List[Post])]
                  | (flatMap / filter)
                  v
[Workers]  RDD[Post]
                  | (flatMap: detección de entidades)
                  v
[Workers]  RDD[NamedEntity]
                  | (map: conversión a clave-valor para conteo)
                  v
[Workers]  RDD[((String, String), Int)]
                  | (reduceByKey: sumatoria global)
                  v
[Workers]  RDD[((String, String), Int)]
                  | (takeOrdered / collect)
                  v
[Driver]   Array[((String, String), Int)]
```

---

### Inciso B - Abstracciones de Spark para cada paso

En primer lugar, se presenta un resumen de las tres abstracciones de Spark que se emplearán:

- **`map`**: Transforma cada elemento del RDD en **exactamente un** elemento de salida. Se utiliza cuando la transformación es uno a uno y cada elemento se procesa de manera independiente.
- **`flatMap`**: Transforma cada elemento del RDD en **cero o más** elementos de salida. Funciona de manera similar a `map`, pero permite devolver una cantidad variable de resultados y los aplana automáticamente.
- **`reduceByKey`**: Agrupa los elementos por clave y los combina mediante una funci�n asociativa (por ejemplo, la suma). Se emplea cuando el resultado depende de **todos los elementos** que comparten una misma clave y no de uno solo.

#### Paso 1: Lectura de suscripciones (Driver)

- **`map`:** No. Se trata de operaciones de E/S básicas ejecutadas por el driver; aún no existe un RDD.
- **`flatMap`:** No.
- **`reduceByKey`:** No.

  **No corresponde a ninguna abstracción.** Es una lectura secuencial de archivo, sin datos distribuidos.

#### Paso 2: Parallelize (Driver -> Workers)

- **`map`:** No. `parallelize` no transforma elementos de forma individual; únicamente distribuye los datos entre los workers.
- **`flatMap`:** No.
- **`reduceByKey`:** No.

  **No corresponde.** Es una operación del `SparkContext` para crear el RDD, no para transformar datos.

#### Paso 3: Descarga y análisis sintáctico de publicaciones (Workers)

- **`map`:** Si. Cada suscripción produce exactamente un resultado `(Boolean, List[Post])`. Se trata de tareas independientes con una relación uno a uno.
- **`flatMap`:** No, pues no es necesario aplanar datos en esta etapa.
- **`reduceByKey`:** No.

  **La abstracci�n adecuada es `map`.**

#### Paso 4: Unificación y filtrado de publicaciones (Workers)

- **`map`:** No. Si se utilizara `map`, se obtendrían listas anidadas (`List[List[Post]]`), lo cual dificultaría el procesamiento posterior.
- **`flatMap`:** Si. Cada tupla contiene una lista de 0 a N publicaciones, y `flatMap` las aplana automáticamente. Posteriormente se aplica un `filter` para eliminar aquellas que estén vacías.
- **`reduceByKey`:** No.

  **La abstracci�n adecuada es `flatMap` + `filter`.**

#### Paso 5: Detección de entidades (Workers)

- **`map`:** No. Una publicación puede contener 0, 1 o varias entidades. Con `map` se generarían listas anidadas.
- **`flatMap`:** Si. Cada publicación produce entre 0 y N entidades, y `flatMap` las aplana automáticamente. El diccionario se propaga como variable de tipo *Broadcast* para que todos los workers dispongan de una copia local.
- **`reduceByKey`:** No.

  **La abstracción adecuada es `flatMap`.**

#### Paso 6: Asignación a clave-valor (Workers)

- **`map`:** Si. Cada entidad se transforma en exactamente un par `((entityType, text), 1)`. La relación es uno a uno, sin necesidad de procesamiento adicional.
- **`flatMap`:** No, dado que la relación es uno a uno.
- **`reduceByKey`:** No en esta etapa; dicha operación se aplica posteriormente.

  **La abstracción adecuada es `map`.**

#### Paso 7: Conteo de ocurrencias (Workers)

- **`map`:** No. Esta operación no es suficiente, pues se requiere combinar todas las ocurrencias de una misma entidad; no basta con examinar una sola.
- **`flatMap`:** No.
- **`reduceByKey`:** Si. Agrupa todos los pares que comparten la misma clave `(entityType, text)` y suma los valores mediante una función asociativa. Esta operación es la indicada para realizar un conteo distribuido.

  **La abstracci�n adecuada es `reduceByKey`.**

#### Paso 8: Clasificación y recolección (Driver)

- **`map`:** No. La ordenación global y la selección de los primeros K elementos requiere consolidar todos los datos.
- **`flatMap`:** No.
- **`reduceByKey`:** No, pues no existen elementos adicionales que reducir.

  **No corresponde a ninguna abstracción.** Se trata de una **acción** de Spark (`takeOrdered`, `collect`), no de una transformación. Las acciones ejecutan el cómputo y transfieren los resultados al driver.

#### Pasos que no se corresponden con las abstracciones y justificación

Los pasos **1**, **2** y **8** no se ajustan a ninguna de las tres abstracciones analizadas por las siguientes razones:

1. **Paso 1 (lectura de suscripciones):** Consiste en operaciones de E/S secuenciales ejecutadas en el driver. No existe un RDD, no se realiza ninguna transformación y no hay paralelismo. Simplemente se cargan los datos de entrada antes de iniciar el procesamiento con Spark.

2. **Paso 2 (parallelize):** `parallelize` no datos, sino que los distribuye desde el driver hacia los workers. No corresponde a `map`, `flatMap` ni `reduceByKey`, sino que pertenece a una categoría distinta: la creación de RDDs.

3. **Paso 8 (clasificación y recolección):** Emplea **acciones** de Spark (`takeOrdered`, `collect`). Las acciones no transforman datos en el clúster, sino que los recuperan hacia el driver para presentar resultados o continuar con procesamiento secuencial.

#### Resumen

| Paso | Abstracción |
| - | - |
| 1. Lectura de suscripciones | *(no aplica; operación del driver)* |
| 2. `parallelize` | *(no aplica; creación del RDD)* |
| 3. Descarga y análisis sintáctico | `map` |
| 4. Unificación y filtrado | `flatMap` + `filter` |
| 5. Detección de entidades | `flatMap` |
| 6. Asignación a clave-valor | `map` |
| 7. Conteo de entidades | `reduceByKey` |
| 8. Clasificación y recolección | *(no aplica; acción de Spark)* |

---

### Inciso C - Barreras de sincronizaci�n e independencia entre workers

- **Dependencia Estrecha** : Permite el procesamiento local sin intercambiar datos.
- **Dependencia Amplia** : Requiere el intercambio de datos a través de la red.

**Pasos con ejecución completamente independiente entre workers:**
Los pasos **3, 4, 5 y 6** pueden ejecutarse de forma completamente paralela e independiente. Las transformaciones involucradas (`map`, `flatMap`, `filter`) operan sobre cada elemento o partición de manera aislada. Un worker no necesita comunicarse ni esperar a los demás para descargar los feeds de la red, parsear el JSON, filtrar publicaciones vacías, buscar entidades nombradas y convertirlas a pares clave-valor iniciales. Toda esta fase inicial del pipeline se ejecuta sin bloqueos entre workers.

**Pasos que constituyen una barrera de sincronización:**

- **Paso 7 (Conteo de entidades mediante `reduceByKey`):** Representa una barrera de sincronización interna del clúster. Para poder sumar todas las ocurrencias de una clave específica (por ejemplo, el lenguaje "Python"), Spark necesita unificar los datos que est�n dispersos. Ning�n worker puede procesar el total final de una clave hasta que **todos** hayan concluido la fase de mapeo y Spark haya intercambiado los datos por la red (shuffle).
- **Paso 8 (Clasificación y recolección):** Al aplicar acciones como `takeOrdered` o `collect`, se crea una barrera global definitiva. El *driver* no puede centralizar, ordenar y mostrar los resultados finales hasta que el cl�ster entero haya finalizado todo el procesamiento distribuido.

---

### Inciso D - Restricciones sobre las funciones pasadas a Spark (Extension points)

Al pasar funciones (o *closures*) a transformaciones como `map` o `reduceByKey`, Spark impone tres restricciones clave para que funcionen correctamente en un entorno distribuido:

1. **Serialización:** Las funciones y los objetos que capturan deben ser **serializables** para poder enviarse por la red desde el *driver* hacia los *workers*.
2. **Estado compartido:** Las variables externas modificadas por la función solo afectan a copias locales del worker. Para compartir estado global, se deben usar **Variables Broadcast** (datos de solo lectura) o **Acumuladores** (contadores concurrentes).
3. **Efectos secundarios:** Las funciones deben ser **puras**. Dado que Spark puede reejecutar tareas ante fallos o retrasarlas por la evaluaci�n perezosa, los efectos secundarios (ej: escribir en BD) pueden ejecutarse m�ltiples veces o en desorden. Para estos casos, se usan acciones explícitas como `foreach`.

---

## Ejercicio 3: Paralelizar el cómputo de entidades nombradas

## Inciso A - reduceByKey como barrera de sincronización 

En el `cluster`, quien en Spark es el conjunto compuesto por el driver y los workers los cuales colaboran para ejecutar *acciónes y transformaciones de Spark sobre particiones de datos*, al llegar a `reduceByKey` lo que ocurre es un cambio de información y datos entre los workers que estuvieron recolectando sus propios pares de clave-valor por separado en las operaciones `flatMap` y `map`, este cambio es necesario pues es la unica operacion capaz de poder juntar estos pares recolectados por separado en cada worker y asi poder obtener un total correcto. 

## Inciso B - Restricciones de reduceByKey 

Las restricciones que impone reduceByKey a la función que le pasamos son la **conmutatividad** y la **asociatividad**, esto debido a que no sabemos como Spark puede distribuir el trabajo decada worker por separado y en que *orden* podria terminar de *juntar* estos resultados, osea, estos valores que coinciden con cada clave, por lo tanto, una función en nuestro caso con una operacion como lo es la suma cumple con estas restricciones. 

## Inciso C - Lectura del diccionario de entidades 

Incialmente, el **driver** es quien lee estas entidades gracias a la funcion *loadAll(entitiesDir: String)*. Luego, el diccionario se distribuye a los workers utilizando una variable broadcast creada mediante *sc.broadcast(dictionary)*. De esta manera, cada worker puede acceder a una copia local del diccionario durante la ejecución de las tareas. 

## Ejercicio 4: Monitoreo del exito de tareas

### Inciso A - Uso de Accumulators, toma de decisiones y valores incorrectos

Los **Accumulators** en Spark operan bajo un modelo de concurrencia donde los *workers* (ejecutores) solo tienen permisos de **escritura** (pueden incrementar el valor mediante `add`), mientras que únicamente el *driver* tiene permisos de **lectura**. Por esta razón, no deben usarse para tomar decisiones lógicas en las etapas distribuidas: los *workers* no pueden leer el valor del acumulador durante la ejecución de las tareas, por lo que carecen del estado global necesario para bifurcar o condicionar su flujo de trabajo en base a ese valor.

**¿En qué situación un Accumulator puede dar un valor incorrecto?**
Esto ocurre típicamente cuando se incrementa un acumulador dentro de una **transformación** (como `map`, `filter` o `flatMap` - como ocurre actualmente en el `Main.scala` del proyecto) en lugar de en una **acción** (como `foreach`). Debido a que Spark es tolerante a fallos y utiliza evaluación perezosa, si un nodo falla y Spark necesita reejecutar una partición perdida, o si un RDD no está persistido y sufre de múltiples acciones que causan la re-evaluación del linaje, la transformación se ejecutará más de una vez. Esto provocará que el acumulador se incremente de forma redundante (sobrestimando el conteo real).

### Inciso B - Disponibilidad del valor para el driver

El valor de un Accumulator está disponible y garantiza ser exacto para el *driver* **únicamente después de que haya finalizado por completo la acción** que evalúa el RDD asociado. 

Antes de que se invoque dicha acción, debido a la evaluación perezosa, el acumulador mantiene su valor inicial (cero). Durante la ejecución de la acción, el valor que observa el *driver* puede ser parcial, ya que los *workers* transmiten los incrementos de manera asíncrona conforme completan sus lotes de tareas. Solamente cuando la acción (por ejemplo, `collect()`, `count()`) concluye exitosamente, Spark asegura que todos los incrementos se han consolidado y el *driver* puede leer el total invocando `.value`.

# Comparación de Rendimiento: Secuencial vs Spark


| Etapa del Pipeline | Tiempo Secuencial (s) | Tiempo con Spark (s) |
| :--- | :---: | :---: |
| **Recolección de posts** (descarga y parseo) | *20.619 s* | *0.06 s* |
| **Conteo de entidades** (detección) | *0.051 s* | *5.482 s* |
| **Recolección de conteos** (agrupación y reduce) | *0.022 s* | *0.707 s* |
| **Tiempo total del pipeline** | *21.035 s* | *6.249 s* |

### Conclusiones y Justificación

**¿Se aprecia la diferencia para la cantidad de datos que estamos trabajando?**
Sí, la diferencia es notable. El tiempo total del pipeline se reduce en más de un 70% (de ~21 segundos a ~6.2 segundos). Esto demuestra que el uso de Spark aporta un gran beneficio de rendimiento al permitir el procesamiento concurrente, siendo especialmente ventajoso para mitigar los cuellos de botella generados por la latencia de las operaciones de red (descarga de los feeds).

**¿Por qué sucede esto?**
Esta reducción en el tiempo total y la particular distribución de los tiempos por etapa en Spark se explican por los siguientes factores:

1. **Evaluación Perezosa (Lazy Evaluation) y Caché:** A diferencia del modelo secuencial donde cada operación se ejecuta de inmediato, Spark posterga el cómputo hasta que se invoca una acción. El tiempo reportado como "Conteo de entidades" (5.482 s) corresponde a la primera acción invocada (`entitiesRDD.count()`). Esta acción obliga a Spark a computar todo el linaje previo, lo que significa que **este tiempo incluye la descarga de todos los feeds, el parseo, el filtrado y la detección de las entidades**. Además, como se usó `cache()` en los RDDs, el paso siguiente ("Recolección de posts", que ejecuta `collect()`) es casi instantáneo (0.06 s) porque Spark simplemente recupera los datos ya procesados desde la memoria.

2. **Paralelización de Operaciones de I/O:** En la versión secuencial, las solicitudes HTTP para descargar los feeds son bloqueantes y se ejecutan una tras otra, acumulando casi 21 segundos de espera. Con Spark, el clúster distribuye y ejecuta múltiples descargas simultáneamente entre sus *workers*, colapsando el tiempo de espera drásticamente.

3. **Overhead de Sincronización (Shuffle):** En contraparte, la recolección de conteos y el agrupamiento son más rápidos en la versión secuencial (0.022 s frente a 0.707 s en Spark). Esto se debe a que la versión secuencial actualiza un diccionario en memoria local, mientras que Spark (`reduceByKey`) requiere una barrera de sincronización y un intercambio de datos entre los nodos de la red (*Shuffle*) para poder sumar todas las claves globales. Aun así, el tiempo que se ahorra paralelizando las descargas compensa con creces este pequeño *overhead*.

## Ejercicio 5: Acceso a datos y estadísticas del resultado

### Inciso A - ¿Qué ocurriría si no se llamara a `cache()`? ¿Cuántas veces se ejecutaría la descarga de feeds?

Sin `cache()`, Spark recomputa todo el linaje de un RDD cada vez que se invoca una acción sobre él o sobre un RDD derivado. En nuestro pipeline, `postsRDD` se referencia en dos puntos distintos: primero cuando `entitiesRDD` aplica `flatMap` sobrel, y luego cuando se llama a `postsRDD.collect()`. Sin persistencia, cada una de esas acciones relanza todo el pipeline desde el principio, incluyendo la descarga HTTP de cada feed. Si hay N suscripciones, los feeds se descargaran 2N veces en lugar de N. Esto no solo desperdicia ancho de banda y tiempo, sino que puede producir resultados inconsistentes si el contenido de los feeds cambia entre ejecuciones.

De forma análoga, `entitiesRDD` es referenciado por `typePairsRDD`, `entityPairsRDD` y la acción `entitiesRDD.count()`. Sin `cache()`, el `flatMap` de detección de entidades (y toda la cadena que le antecede, incluyendo las descargas) se reejecutara tres veces.

### Inciso B - ¿Por qué es incorrecto llamar a `collect()` entre los pasos a) y b) del ejercicio 3 y luego continuar el pipeline?

Llamar a `collect()` entre la extracción de entidades (`flatMap`) y la asignación a pares clave-valor (`map`) trae todos los datos al driver como una colección local de Scala. Para continuar el pipeline habra que volver a crear un RDD con `sc.parallelize(...)` a partir de esa colección local. Esto tiene dos consecuencias negativas:

1. **Rompe la distribución del trabajo:** `sc.parallelize` redistribuye los datos desde el driver, lo que genera trfico de red innecesario y puede crear un cuello de botella si la cantidad de datos es grande. Adems, el nmero de particiones resultante puede no serptimo.
2. **Elimina el paralelismo del pipeline:** Las transformaciones previas al `collect()` ya se materializaron en el driver. Las operaciones subsiguientes (`map`, `reduceByKey`) tendran que partir de cero con un RDD redistribuido artificialmente, perdiendo el locality de los datos y el schedulingptimo que Spark habra calculado para el pipeline completo.

La alternativa correcta es encadenar todas las transformaciones y dejar que Spark construya el plan de ejecucin completo, interviniendo solo al final con una acción terminal.

### Inciso C - `cache()` es también lazy, ¿en qué momento se almacena realmente el RDD en memoria?

Llamar a `.cache()` sobre un RDD no almacena ningún dato de inmediato: solo anota en el plan de ejecución que ese RDD debe persistirse cuando sea computado. El almacenamiento real ocurre la primera vez que una acción terminal (`count`, `collect`, `take`, etc.) desencadena la evaluación del RDD. En ese momento Spark materializa las particiones y las guarda en memoria. Las acciones posteriores sobre el mismo RDD encuentran las particiones ya almacenadas y las leen directamente, sin recomputar el linaje.
