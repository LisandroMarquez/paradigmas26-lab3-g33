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
- **Salida:** `RDD[(Boolean, List[Post])]` *(tupla que indica si la descarga del feed fue exitosa y las publicaciones extraí­das)*

#### Paso 4: Unificación y filtrado de publicaciones (Workers)

- **Transformación:** Extracción y consolidación de todas las publicaciones provenientes de las distintas listas (`flatMap`), seguida del filtrado de aquellas que se encuentren vací­as o no sean válidas (`filter`).
- **Entrada:** `RDD[(Boolean, List[Post])]`
- **Salida:** `RDD[Post]`

#### Paso 5: Extracción y clasificación de entidades (Workers)

- **Transformación:** Análisis del texto combinado de cada publicación mediante el diccionario para identificar entidades nombradas (`flatMap`). El diccionario debe propagarse utilizando, por ejemplo, variables de tipo *Broadcast*.
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
- **`reduceByKey`**: Agrupa los elementos por clave y los combina mediante una función asociativa (por ejemplo, la suma). Se emplea cuando el resultado depende de **todos los elementos** que comparten una misma clave y no de uno solo.

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

  **La abstracción adecuada es `map`.**

#### Paso 4: Unificación y filtrado de publicaciones (Workers)

- **`map`:** No. Si se utilizara `map`, se obtendrí­an listas anidadas (`List[List[Post]]`), lo cual dificultarí­a el procesamiento posterior.
- **`flatMap`:** Si. Cada tupla contiene una lista de 0 a N publicaciones, y `flatMap` las aplana automáticamente. Posteriormente se aplica un `filter` para eliminar aquellas que estén vací­as.
- **`reduceByKey`:** No.

  **La abstracción adecuada es `flatMap` + `filter`.**

#### Paso 5: Detección de entidades (Workers)

- **`map`:** No. Una publicación puede contener 0, 1 o varias entidades. Con `map` se generarí­an listas anidadas.
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

  **La abstracción adecuada es `reduceByKey`.**

#### Paso 8: Clasificación y recolección (Driver)

- **`map`:** No. La ordenación global y la selección de los primeros K elementos requiere consolidar todos los datos.
- **`flatMap`:** No.
- **`reduceByKey`:** No, pues no existen elementos adicionales que reducir.

  **No corresponde a ninguna abstracción.** Se trata de una **acción** de Spark (`takeOrdered`, `collect`), no de una transformación. Las acciones ejecutan el cómputo y transfieren los resultados al driver.

#### Pasos que no se corresponden con las abstracciones y justificación

Los pasos **1**, **2** y **8** no se ajustan a ninguna de las tres abstracciones analizadas por las siguientes razones:

1. **Paso 1 (lectura de suscripciones):** Consiste en operaciones de E/S secuenciales ejecutadas en el driver. No existe un RDD, no se realiza ninguna transformación y no hay paralelismo. Simplemente se cargan los datos de entrada antes de iniciar el procesamiento con Spark.

2. **Paso 2 (parallelize):** `parallelize` no datos, sino que los distribuye desde el driver hacia los workers. No corresponde a `map`, `flatMap` ni `reduceByKey`, sino que pertenece a una categorí­a distinta: la creación de RDDs.

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

### Inciso C - Barreras de sincronización e independencia entre workers

- **Dependencia Estrecha** : Permite el procesamiento local sin intercambiar datos.
- **Dependencia Amplia** : Requiere el intercambio de datos a través de la red.

**Pasos con ejecución completamente independiente entre workers:**
Los pasos **3, 4, 5 y 6** pueden ejecutarse de forma completamente paralela e independiente. Las transformaciones involucradas (`map`, `flatMap`, `filter`) operan sobre cada elemento o partición de manera aislada. Un worker no necesita comunicarse ni esperar a los demás para descargar los feeds de la red, parsear el JSON, filtrar publicaciones vací­as, buscar entidades nombradas y convertirlas a pares clave-valor iniciales. Toda esta fase inicial del pipeline se ejecuta sin bloqueos entre workers.

**Pasos que constituyen una barrera de sincronización:**

- **Paso 7 (Conteo de entidades mediante `reduceByKey`):** Representa una barrera de sincronización interna del clúster. Para poder sumar todas las ocurrencias de una clave especí­fica (por ejemplo, el lenguaje "Python"), Spark necesita unificar los datos que están dispersos. Ningún worker puede procesar el total final de una clave hasta que **todos** hayan concluido la fase de mapeo y Spark haya intercambiado los datos por la red (shuffle).
- **Paso 8 (Clasificación y recolección):** Al aplicar acciones como `takeOrdered` o `collect`, se crea una barrera global definitiva. El *driver* no puede centralizar, ordenar y mostrar los resultados finales hasta que el clúster entero haya finalizado todo el procesamiento distribuido.

---

### Inciso D - Restricciones sobre las funciones pasadas a Spark (Extension points)

Al pasar funciones (o *closures*) a transformaciones como `map` o `reduceByKey`, Spark impone tres restricciones clave para que funcionen correctamente en un entorno distribuido:

1. **Serialización:** Las funciones y los objetos que capturan deben ser **serializables** para poder enviarse por la red desde el *driver* hacia los *workers*.
2. **Estado compartido:** Las variables externas modificadas por la función solo afectan a copias locales del worker. Para compartir estado global, se deben usar **Variables Broadcast** (datos de solo lectura) o **Acumuladores** (contadores concurrentes).
3. **Efectos secundarios:** Las funciones deben ser **puras**. Dado que Spark puede reejecutar tareas ante fallos o retrasarlas por la evaluación perezosa, los efectos secundarios (ej: escribir en BD) pueden ejecutarse múltiples veces o en desorden. Para estos casos, se usan acciones explí­citas como `foreach`.
