# Informe - Laboratorio 3

## Ejercicio 1: Identificaci�n de las regiones paralelizables

### Inciso A - Diagrama de flujo y grafo de dependencias

A continuaci�n se describe el flujo de la aplicaci�n utilizando Apache Spark, diferenciando los pasos que coordina el `driver` de aquellos que se distribuyen y ejecutan en paralelo en los distintos `workers`.

#### Paso 1: Lectura de suscripciones (Driver)

- **Acci�n:** Lectura del archivo JSON local mediante funciones de E/S tradicionales.
- **Salida:** `List[Subscription]`

#### Paso 2: Paralelizaci�n (Driver -> Workers)

- **Acci�n/Transformaci�n:** Conversi�n de la colecci�n est�tica de suscripciones en un RDD para distribuir la carga de trabajo (`parallelize`).
- **Entrada:** `List[Subscription]`
- **Salida:** `RDD[Subscription]`

#### Paso 3: Descarga y an�lisis sint�ctico de publicaciones (Workers)

- **Transformaci�n:** Descarga del contenido de los feeds a trav�s de la red y an�lisis sint�ctico del JSON de las publicaciones (`map`). Esta etapa es altamente paralelizable.
- **Entrada:** `RDD[Subscription]`
- **Salida:** `RDD[(Boolean, List[Post])]` *(tupla que indica si la descarga del feed fue exitosa y las publicaciones extra��das)*

#### Paso 4: Unificaci�n y filtrado de publicaciones (Workers)

- **Transformaci�n:** Extracci�n y consolidaci�n de todas las publicaciones provenientes de las distintas listas (`flatMap`), seguida del filtrado de aquellas que se encuentren vac��as o no sean v�lidas (`filter`).
- **Entrada:** `RDD[(Boolean, List[Post])]`
- **Salida:** `RDD[Post]`

#### Paso 5: Extracci�n y clasificaci�n de entidades (Workers)

- **Transformaci�n:** An�lisis del texto combinado de cada publicaci�n mediante el diccionario para identificar entidades nombradas (`flatMap`). El diccionario debe propagarse utilizando, por ejemplo, variables de tipo *Broadcast*.
- **Entrada:** `RDD[Post]`
- **Salida:** `RDD[NamedEntity]`

#### Paso 6: Asignaci�n para conteo (Workers)

- **Transformaci�n:** Asignaci�n de cada entidad identificada a un par clave-valor, utilizando el tipo y nombre de la entidad como clave, con un valor inicial de 1 para su posterior sumatoria (`map`).
- **Entrada:** `RDD[NamedEntity]`
- **Salida:** `RDD[((String, String), Int)]`

#### Paso 7: Agrupaci�n y conteo (Workers)

- **Transformaci�n:** Sumatoria de las ocurrencias de las entidades que comparten una misma clave, realizada de forma distribuida (`reduceByKey`).
- **Entrada:** `RDD[((String, String), Int)]`
- **Salida:** `RDD[((String, String), Int)]`

#### Paso 8: Clasificaci�n y recolecci�n (Driver)

- **Acci�n:** Extracci�n de los resultados y transferencia al programa principal, orden�ndolos de forma descendente para obtener las entidades m�s frecuentes (`takeOrdered` o mediante un `sortBy` seguido de un `take`).
- **Entrada:** `RDD[((String, String), Int)]`
- **Salida:** `Array[((String, String), Int)]`

#### Resumen gr�fico del flujo

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
                  | (flatMap: detecci�n de entidades)
                  v
[Workers]  RDD[NamedEntity]
                  | (map: conversi�n a clave-valor para conteo)
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

En primer lugar, se presenta un resumen de las tres abstracciones de Spark que se emplear�n:

- **`map`**: Transforma cada elemento del RDD en **exactamente un** elemento de salida. Se utiliza cuando la transformaci�n es uno a uno y cada elemento se procesa de manera independiente.
- **`flatMap`**: Transforma cada elemento del RDD en **cero o m�s** elementos de salida. Funciona de manera similar a `map`, pero permite devolver una cantidad variable de resultados y los aplana autom�ticamente.
- **`reduceByKey`**: Agrupa los elementos por clave y los combina mediante una funci�n asociativa (por ejemplo, la suma). Se emplea cuando el resultado depende de **todos los elementos** que comparten una misma clave y no de uno solo.

#### Paso 1: Lectura de suscripciones (Driver)

- **`map`:** No. Se trata de operaciones de E/S b�sicas ejecutadas por el driver; a�n no existe un RDD.
- **`flatMap`:** No.
- **`reduceByKey`:** No.

  **No corresponde a ninguna abstracci�n.** Es una lectura secuencial de archivo, sin datos distribuidos.

#### Paso 2: Parallelize (Driver -> Workers)

- **`map`:** No. `parallelize` no transforma elementos de forma individual; �nicamente distribuye los datos entre los workers.
- **`flatMap`:** No.
- **`reduceByKey`:** No.

  **No corresponde.** Es una operaci�n del `SparkContext` para crear el RDD, no para transformar datos.

#### Paso 3: Descarga y an�lisis sint�ctico de publicaciones (Workers)

- **`map`:** Si. Cada suscripci�n produce exactamente un resultado `(Boolean, List[Post])`. Se trata de tareas independientes con una relaci�n uno a uno.
- **`flatMap`:** No, pues no es necesario aplanar datos en esta etapa.
- **`reduceByKey`:** No.

  **La abstracci�n adecuada es `map`.**

#### Paso 4: Unificaci�n y filtrado de publicaciones (Workers)

- **`map`:** No. Si se utilizara `map`, se obtendr��an listas anidadas (`List[List[Post]]`), lo cual dificultar��a el procesamiento posterior.
- **`flatMap`:** Si. Cada tupla contiene una lista de 0 a N publicaciones, y `flatMap` las aplana autom�ticamente. Posteriormente se aplica un `filter` para eliminar aquellas que est�n vac��as.
- **`reduceByKey`:** No.

  **La abstracci�n adecuada es `flatMap` + `filter`.**

#### Paso 5: Detecci�n de entidades (Workers)

- **`map`:** No. Una publicaci�n puede contener 0, 1 o varias entidades. Con `map` se generar��an listas anidadas.
- **`flatMap`:** Si. Cada publicaci�n produce entre 0 y N entidades, y `flatMap` las aplana autom�ticamente. El diccionario se propaga como variable de tipo *Broadcast* para que todos los workers dispongan de una copia local.
- **`reduceByKey`:** No.

  **La abstracci�n adecuada es `flatMap`.**

#### Paso 6: Asignaci�n a clave-valor (Workers)

- **`map`:** Si. Cada entidad se transforma en exactamente un par `((entityType, text), 1)`. La relaci�n es uno a uno, sin necesidad de procesamiento adicional.
- **`flatMap`:** No, dado que la relaci�n es uno a uno.
- **`reduceByKey`:** No en esta etapa; dicha operaci�n se aplica posteriormente.

  **La abstracci�n adecuada es `map`.**

#### Paso 7: Conteo de ocurrencias (Workers)

- **`map`:** No. Esta operaci�n no es suficiente, pues se requiere combinar todas las ocurrencias de una misma entidad; no basta con examinar una sola.
- **`flatMap`:** No.
- **`reduceByKey`:** Si. Agrupa todos los pares que comparten la misma clave `(entityType, text)` y suma los valores mediante una funci�n asociativa. Esta operaci�n es la indicada para realizar un conteo distribuido.

  **La abstracci�n adecuada es `reduceByKey`.**

#### Paso 8: Clasificaci�n y recolecci�n (Driver)

- **`map`:** No. La ordenaci�n global y la selecci�n de los primeros K elementos requiere consolidar todos los datos.
- **`flatMap`:** No.
- **`reduceByKey`:** No, pues no existen elementos adicionales que reducir.

  **No corresponde a ninguna abstracci�n.** Se trata de una **acci�n** de Spark (`takeOrdered`, `collect`), no de una transformaci�n. Las acciones ejecutan el c�mputo y transfieren los resultados al driver.

#### Pasos que no se corresponden con las abstracciones y justificaci�n

Los pasos **1**, **2** y **8** no se ajustan a ninguna de las tres abstracciones analizadas por las siguientes razones:

1. **Paso 1 (lectura de suscripciones):** Consiste en operaciones de E/S secuenciales ejecutadas en el driver. No existe un RDD, no se realiza ninguna transformaci�n y no hay paralelismo. Simplemente se cargan los datos de entrada antes de iniciar el procesamiento con Spark.

2. **Paso 2 (parallelize):** `parallelize` no datos, sino que los distribuye desde el driver hacia los workers. No corresponde a `map`, `flatMap` ni `reduceByKey`, sino que pertenece a una categor��a distinta: la creaci�n de RDDs.

3. **Paso 8 (clasificaci�n y recolecci�n):** Emplea **acciones** de Spark (`takeOrdered`, `collect`). Las acciones no transforman datos en el cl�ster, sino que los recuperan hacia el driver para presentar resultados o continuar con procesamiento secuencial.

#### Resumen

| Paso | Abstracci�n |
| - | - |
| 1. Lectura de suscripciones | *(no aplica; operaci�n del driver)* |
| 2. `parallelize` | *(no aplica; creaci�n del RDD)* |
| 3. Descarga y an�lisis sint�ctico | `map` |
| 4. Unificaci�n y filtrado | `flatMap` + `filter` |
| 5. Detecci�n de entidades | `flatMap` |
| 6. Asignaci�n a clave-valor | `map` |
| 7. Conteo de entidades | `reduceByKey` |
| 8. Clasificaci�n y recolecci�n | *(no aplica; acci�n de Spark)* |

---

### Inciso C - Barreras de sincronizaci�n e independencia entre workers

- **Dependencia Estrecha** : Permite el procesamiento local sin intercambiar datos.
- **Dependencia Amplia** : Requiere el intercambio de datos a trav�s de la red.

**Pasos con ejecuci�n completamente independiente entre workers:**
Los pasos **3, 4, 5 y 6** pueden ejecutarse de forma completamente paralela e independiente. Las transformaciones involucradas (`map`, `flatMap`, `filter`) operan sobre cada elemento o partici�n de manera aislada. Un worker no necesita comunicarse ni esperar a los dem�s para descargar los feeds de la red, parsear el JSON, filtrar publicaciones vac��as, buscar entidades nombradas y convertirlas a pares clave-valor iniciales. Toda esta fase inicial del pipeline se ejecuta sin bloqueos entre workers.

**Pasos que constituyen una barrera de sincronizaci�n:**

- **Paso 7 (Conteo de entidades mediante `reduceByKey`):** Representa una barrera de sincronizaci�n interna del cl�ster. Para poder sumar todas las ocurrencias de una clave espec��fica (por ejemplo, el lenguaje "Python"), Spark necesita unificar los datos que est�n dispersos. Ning�n worker puede procesar el total final de una clave hasta que **todos** hayan concluido la fase de mapeo y Spark haya intercambiado los datos por la red (shuffle).
- **Paso 8 (Clasificaci�n y recolecci�n):** Al aplicar acciones como `takeOrdered` o `collect`, se crea una barrera global definitiva. El *driver* no puede centralizar, ordenar y mostrar los resultados finales hasta que el cl�ster entero haya finalizado todo el procesamiento distribuido.

---

### Inciso D - Restricciones sobre las funciones pasadas a Spark (Extension points)

Al pasar funciones (o *closures*) a transformaciones como `map` o `reduceByKey`, Spark impone tres restricciones clave para que funcionen correctamente en un entorno distribuido:

1. **Serializaci�n:** Las funciones y los objetos que capturan deben ser **serializables** para poder enviarse por la red desde el *driver* hacia los *workers*.
2. **Estado compartido:** Las variables externas modificadas por la funci�n solo afectan a copias locales del worker. Para compartir estado global, se deben usar **Variables Broadcast** (datos de solo lectura) o **Acumuladores** (contadores concurrentes).
3. **Efectos secundarios:** Las funciones deben ser **puras**. Dado que Spark puede reejecutar tareas ante fallos o retrasarlas por la evaluaci�n perezosa, los efectos secundarios (ej: escribir en BD) pueden ejecutarse m�ltiples veces o en desorden. Para estos casos, se usan acciones expl��citas como `foreach`.

---

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
