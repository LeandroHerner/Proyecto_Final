# Modelo Económico de la empresa

## Modelo de incorporación de vehículos en función del tiempo

Suponiendo que el plazo de inversión es entre 2025 (primer año fiscal luego de la presentación de este proyecto) y 2030 (año en que las flotas de taxis tienen que ser obligadamente eléctricas) se puede pensar de ir incorporando taxis eléctricos de a poco. Por ejemplo, dividiendo el período 2025...2030 en "j" tramos donde en cada tramo se cambiarán (N / j) taxis de combustión por eléctricos donde N es el número total de taxis de la empresa, entonces la función $s(t)$ siguiente es la que modela la cantidad de taxis eléctricos de la empresa en función del tiempo.

$$
s(t) = \displaystyle\sum_{i=1}^j \frac{N * \delta\left(2025 + 5 \frac{(i - 1)}{j} - t\right)}{ j}
$$

como ejemplo, en la siguiente figura se representa la función anterior con N = 50 y j = 5 (o sea, recambio de 50 vehículos en 5 etapas equidistantes en tiempo)

![Incorporacion EV](/images/ModeloEconomicoplot2d1.png)

En contrapartida, la incorporación de estos vehículos elétricos es para reemplazar vehículos de motor a explosión, lo cual se encontraría modelado por la siguente ecuación

$$
r(t) = N * \delta(t-2024) - \displaystyle\sum_{i=1}^j \frac{N * \delta\left(2025 + 5 * \frac{(i - 1)}{j} - t \right)}{ j}
$$

y siguiendo el ejemplo anterior entonces para ese caso la evolución de autos a explosión en la flota sería

![reduccion std](/images/ModeloEconomicoplot2d2.png)


## Costo operacional de la flota de taxis
Ahora veamos como cambiaría el costo de operación de la flota de taxis en función del tiempo con el modelo de incorporación de vehiculos eléctricos propuesto arriba. Sean $r(t)$ el número de taxis comunes, $s(t)$ el número de taxis eléctricos, $MEx(t)$ y $MEv(t)$ los costos operacionales de los taxis comunes y eléctricos, respectivamente.

$$
CostoOp(t) = \left(N - \int_{2023}^x \displaystyle\sum_{i=1}^j \frac{N * \delta(2025 + 5 * \frac{(i - 1)}{j} - t)}{ j} dt \right)*MEx + \left( \int_{2023}^x \displaystyle\sum_{i=1}^j \frac{N * \delta(2025 + 5 * \frac{(i - 1)}{j} - t)}{ j} dt \right)*MEv
$$

Otra vez, para ejemplificar suponiendo recambio de 50 vehículos en 5 etapas, con costos de operación de 500 y 250 dólares (los valores reales estarán en el respectivo informe de análisis de datos), se obtiene la siguiente evolución de costos de operación de la flota.

![costo operativo](/images/ModeloEconomicoplot2d3.png)


El costo operacional total en un período de tiempo dado resulta de integrar $CostoOp(t)$ en el rango de tiempo deseado. Si para el año 2030 se desea haber realizado el cambio por completo entonces se puede evaluar el costo total como:

$$
CostoOpTotal = \int_{2023}^{2030} CostoOp(t) dt
$$

Con esta última ecuación es posible determinar cuál es la cantidad de etapas de recambio óptima ($j$), o sea, se puede minimizar la ecuación anterior en funcion de $j$ y monto de inversión inicial (teniendo en cuenta que $j$ es una variable discreta).


## Modelo de Inversión
Supongamos que para cambiar taxis en cada recambio se pide un prestamo a 2 años, entonces el costo de recambio en cada interacción será la del precio de compra del vehículo más interés repartido en los 2 años siguientes a la incorporación. Esto lo modelamos con una doble Heaviside. Siendo $PrecEv$ el precio de lista de un vehículo eléctrico més interés bancario y que se encuentra dividido por 24 para simular pagos mensuales por 2 años del préstamo.

$$
inv = \displaystyle\sum_{i=1}^j \left[\frac{N}{j} \frac{PrecEv}{24} \left[Heaviside\left(t- \left(2025 + \frac{5*(i-1)}{j} \right) \right) - Heaviside\left(t- \left(2025 + \frac{5*(i-1)}{j} + 2 \right)\right)\right]\right]
$$


Siguiente el ejemplo, suponiendo además $PrecEv$ de 50000 dólares y una tasa de 5% entonces se obtiene la siguiente figura que modela el perfil de pagos mensuales de los créditos a tomar para realizar el recambio de vehículos.

![costo operativo](/images/ModeloEconomicoplot2d5.png)


Relacionando este modelo de inversión con el de costo operativo y se minimiza el ecuación resultante se obtiene que la cantidad de etapas de incorporación óptima es $j = 5$.

