# opensky

We have pre-loaded data in opensky-data-2019-08-27, if you need more data -- run
```
$ python3 main.py
```
it will retrieve data each 10 secs and store it partitioned by 5 minutes for one file and by 1 hour for one folder.

## How to run spark queries?
```
$ cd spark-queries
$ sbt package
$ spark-submit --class SparkQueries target/scala-2.11/spark-queries_2.11-0.1.jar
```
Note, that you should change path to datasource to
```
file:///path/to/project/directory/opensky-data-2019-08-27/*/*
```

### a. Get number of partitions for 1-hour dataset
```
val rdd = spark.sparkContext.textFile("file:///home/kharivitalij/Projects/opensky/opensky-data-2019-08-27/14/*")
Num partitions: 9
```

```
val rowDf = spark.read.format("csv").load("file:///home/kharivitalij/Projects/opensky/opensky-data-2019-08-27/*/*")
rowDf.count = 3934110
```

### b. Calculate average latitude and minimum longitude for each origin _country
```
df.groupBy("origin_country").agg(avg("latitude")).show(1000, false)
+--------------------+-------------------+
|      origin_country|      avg(latitude)|
+--------------------+-------------------+
|            Paraguay|               null|
|               Yemen| 18.731909523809527|
|Islamic Republic ...|  36.96395956828562|
|             Senegal|  13.83482529118137|
|              Sweden|  56.63828127836008|
|   Republic of Korea|  36.15217383113181|
|         Philippines| 18.456260628731176|
|           Singapore| 21.289015527089205|
|            Malaysia|  5.157234106094362|
|Kingdom of the Ne...| 49.395846615032376|
|                Fiji| -36.02339613259669|
|              Turkey| 43.957109868534246|
|                Iraq|  47.20939151391719|
|             Germany|  48.92151283685763|
|         Afghanistan| 26.757747422680414|
|            Cambodia| 20.371078398791543|
|              Jordan| 44.505118648390166|
|              Rwanda| 40.468327397260275|
|            Maldives|               null|
|              France|  46.46282603505439|
|              Greece|  41.63472024953464|
|           Sri Lanka| 20.120338932213564|
|              Taiwan|  26.18428935943336|
|             Algeria|  42.92474601557816|
|                Togo| 25.542253225806462|
|            Slovakia| 42.478406618209654|
|                null|  46.52848472286913|
|           Argentina| -27.76028845783131|
|             Belgium|  47.10715661761595|
|              Angola|-25.626924542124545|
|          San Marino|  44.52274054855436|
|             Ecuador| -6.387109488139823|
|               Qatar|  33.41336636010962|
|             Albania|  44.01342724505327|
|          Madagascar| 49.721120000000006|
|             Finland|  58.67973792533075|
|               Ghana|  42.00667730769231|
|             Myanmar| 16.073839003436415|
|   Brunei Darussalam|  7.018601705930145|
|                Peru|  -14.5909082278481|
|       United States|  37.72613552267891|
|               India| 21.963100312174745|
|               China| 30.226877537831115|
|             Belarus|  49.33467057753517|
|              Kuwait|  41.21931100430889|
|               Malta|  44.55258290341659|
|               Chile|0.09633024850043433|
|          Tajikistan|  54.54297887323944|
|             Croatia|  44.75111040312097|
|             Bolivia|-2.8208434138737344|
|               Gabon|               null|
|               Italy| 41.380008606287525|
|            Suriname|  8.924114942528735|
|           Lithuania|  48.30698219257542|
|              Norway|  57.18436465354462|
|        Turkmenistan|  41.03554642857143|
|               Spain|  41.67036590917538|
|             Denmark| 52.381068558316656|
|               Niger| 52.737667499999986|
|          Bangladesh|  23.20095382113821|
|  Russian Federation|  54.45832365874349|
|             Ireland| 46.739526094858334|
|            Thailand| 18.985806177410563|
|             Morocco|  41.38263664137385|
|          Cape Verde|  30.65376382978723|
|              Panama| -8.208964749733761|
|             Ukraine| 46.673828484952004|
|             Iceland| 51.921834053384615|
|              Israel|  41.35086673443707|
|                Oman| 24.629158311628597|
|              Cyprus|  35.90409265175721|
|              Mexico|  37.77971481891068|
|       C�te d'Ivoire|  39.40484879518074|
|             Estonia|  53.26019110446921|
|          Montenegro|  48.59320636474908|
|             Georgia| 41.290300917431196|
|           Indonesia| 0.7577417192812044|
|Libyan Arab Jamah...| 34.926208156028395|
|            Mongolia|  50.34134904458599|
|          Azerbaijan|  44.46876808413352|
|             Armenia|  48.65147299107144|
|             Tunisia| 43.651056459893006|
|            Honduras|               null|
| Trinidad and Tobago| 25.586323088023104|
|        Saudi Arabia| 30.848582039030905|
|             Namibia|-24.803563265306128|
|         Switzerland|  46.97169292853963|
|            Ethiopia|  27.97236636915387|
|              Latvia|  51.65434010985046|
|               ICAO1|  35.72232738095237|
|United Arab Emirates|  30.72587835320997|
|              Canada| 45.531426352733924|
|          Seychelles| -25.26179083969465|
|          Uzbekistan|  45.36792846177468|
|          Kyrgyzstan|  44.86093478260869|
|      Czech Republic|  46.45974054583289|
|              Brazil|-18.042068379521368|
|               Kenya|  42.29199912485415|
|             Lebanon| 40.323970751341704|
|Lao People's Demo...| 18.011627015250543|
|            Slovenia| 46.668294733461806|
| Antigua and Barbuda| 12.935182397959185|
|            Viet Nam|  24.67596166339463|
|               Japan|  36.72344018270473|
|            Botswana|  1.340178717201168|
|          Luxembourg|  45.98867849295014|
|         New Zealand| -31.46351634899331|
|              Poland|  50.59542836479905|
|            Portugal| 44.608786638268484|
| Republic of Moldova|  49.18670815366523|
|           Australia| -28.24589757945055|
|    Papua New Guinea|   37.3495928994083|
|             Romania|  44.43648636573004|
|            Bulgaria|  43.19591864356138|
|             Austria|  46.60201012317717|
|               Nepal| 21.868072802197798|
|               Egypt|  42.11854047026624|
|          Kazakhstan| 44.122935111411195|
|              Serbia|  45.02038885514017|
|        South Africa|-21.556141743311052|
|             Bahrain| 31.455590171211124|
|            Colombia|-11.605762882096068|
|             Hungary|  48.78650127346987|
|            Pakistan|  35.83060615747694|
|           Mauritius|-26.005386923076927|
|      United Kingdom|  49.55323377277231|
|Syrian Arab Republic| 25.505387450980418|
|Democratic People...| 38.480553061224484|
+--------------------+-------------------+

df.groupBy("origin_country").agg(min("longitude")).show(1000, false)
+--------------------+--------------+
|      origin_country|min(longitude)|
+--------------------+--------------+
|            Paraguay|          None|
|               Yemen|       69.9016|
|Islamic Republic ...|        1.8992|
|             Senegal|      -13.2221|
|              Sweden|       -0.0057|
|   Republic of Korea|       -0.3543|
|         Philippines|      100.0109|
|            Malaysia|      100.0004|
|           Singapore|      -56.5634|
|                Fiji|      144.8527|
|Kingdom of the Ne...|       -0.0005|
|              Turkey|       -0.0026|
|                Iraq|       10.0288|
|             Germany|       -0.0003|
|         Afghanistan|        48.546|
|            Cambodia|      100.7368|
|              Jordan|       -0.0069|
|            Maldives|          None|
|              Rwanda|        7.0604|
|              France|       -0.0001|
|              Greece|       -0.0007|
|           Sri Lanka|      100.0034|
|              Taiwan|     -124.0324|
|             Algeria|       -0.0002|
|                Togo|      117.6848|
|                null|       11.2627|
|            Slovakia|       -0.0049|
|           Argentina|      -10.0151|
|              Angola|       28.0685|
|             Belgium|       -0.0018|
|          San Marino|       -0.0076|
|             Ecuador|      -63.1507|
|               Qatar|       -0.0029|
|             Albania|       10.0343|
|          Madagascar|       10.7308|
|             Finland|       -0.0009|
|               Ghana|       16.4322|
|             Myanmar|      100.0059|
|   Brunei Darussalam|      101.4873|
|                Peru|      -71.5495|
|               China|       -0.0064|
|               India|       -0.0042|
|       United States|       -0.0001|
|             Belarus|       10.0041|
|              Kuwait|        -0.001|
|               Malta|       -0.0002|
|               Chile|       -0.1141|
|          Tajikistan|       36.2134|
|             Croatia|       10.0173|
|             Bolivia|       -3.4775|
|               Gabon|          None|
|               Italy|     -118.3954|
|            Suriname|       -59.913|
|           Lithuania|       -0.0065|
|              Norway|       -0.0074|
|        Turkmenistan|       28.7302|
|               Spain|       -0.0001|
|             Denmark|       -0.0029|
|               Niger|       19.2008|
|          Bangladesh|       48.4927|
|  Russian Federation|      -22.5987|
|             Ireland|       -0.0001|
|            Thailand|        -0.017|
|             Morocco|       -0.0013|
|          Cape Verde|      -13.9365|
|              Panama|       -58.558|
|             Ukraine|       -0.0051|
|             Iceland|       -0.0128|
|              Israel|       -0.0018|
|                Oman|      100.0338|
|              Cyprus|       24.1484|
|              Mexico|       -0.8014|
|       C�te d'Ivoire|        2.7352|
|             Estonia|       -0.0018|
|             Georgia|       20.4816|
|          Montenegro|       10.0199|
|           Indonesia|      -13.1268|
|Libyan Arab Jamah...|       12.5614|
|            Mongolia|       10.0262|
|          Azerbaijan|       12.5188|
|             Armenia|       10.0107|
|             Tunisia|       -0.0084|
|            Honduras|          None|
| Trinidad and Tobago|      -59.3167|
|        Saudi Arabia|        -0.002|
|             Namibia|       26.0433|
|         Switzerland|       -0.0008|
|            Ethiopia|      -13.1221|
|               ICAO1|      -78.4247|
|              Latvia|       -0.0016|
|United Arab Emirates|       -0.0019|
|              Canada|       -0.0001|
|          Seychelles|        28.239|
|          Kyrgyzstan|        70.724|
|          Uzbekistan|      -60.0765|
|      Czech Republic|       -0.0007|
|              Brazil|       -0.0012|
|               Kenya|       10.0098|
|             Lebanon|        -0.037|
|Lao People's Demo...|      100.6432|
| Antigua and Barbuda|      -59.1376|
|            Slovenia|       10.0363|
|            Viet Nam|        10.027|
|            Botswana|        26.989|
|               Japan|     -100.0136|
|          Luxembourg|       -0.0006|
|         New Zealand|       -0.0008|
|              Poland|       -0.0012|
|            Portugal|       -0.0001|
| Republic of Moldova|        0.2592|
|           Australia|       -0.0087|
|    Papua New Guinea|       10.0103|
|             Romania|        -0.004|
|            Bulgaria|         0.083|
|             Austria|       -0.0016|
|               Nepal|      110.8564|
|               Egypt|       -0.0053|
|          Kazakhstan|        45.326|
|              Serbia|       -0.4647|
|        South Africa|       18.4378|
|             Bahrain|       -0.0108|
|            Colombia|      -71.5664|
|             Hungary|       -0.0055|
|            Pakistan|       11.4009|
|           Mauritius|       28.2402|
|Syrian Arab Republic|        48.548|
|      United Kingdom|       -0.0001|
|Democratic People...|       122.661|
+--------------------+--------------+
```

### c. Get the max speed ever seen for the last 4 hours
```
df.filter(allAfter(4)).agg(max("velocity")).show
+-------------+
|max(velocity)|
+-------------+
|         None|
+-------------+
```

### d. Get top 10 airplanes with max average speed for the last 4 hours (round the result)
```
df.filter(allAfter(4)).groupBy("icao24").agg(avg("velocity")).sort(col("avg(velocity)").desc).limit(10).show
+------+------------------+
|icao24|     avg(velocity)|
+------+------------------+
|33859a| 694.3645833333334|
|acddf8| 471.5472413793105|
|780b14| 307.1553571428572|
|a2ca64|  304.710243902439|
|789226| 297.5607344632768|
|489586| 296.2183763837639|
|780d23|293.31753246753254|
|e47e74| 291.8961165048545|
|780b7d|291.65835443037975|
|78029f|290.73890410958904|
+------+------------------+
```

### e. Show distinct airplanes where origin_country = 'Germany' and it was on ground at least one time during last 4 hours.
```
df.filter(allAfter(4) && col("origin_country") === "Germany" && col("on_ground") === "True").select("icao24").distinct.show
+------+
|icao24|
+------+
|3e0a46|
|3c0a5a|
|3defea|
|3c42f6|
|3c66b7|
|3c662c|
|3c6573|
|3c56a8|
|3c56ee|
|3c65cd|
|3c5ee8|
|3de586|
|3c66b5|
|3d2026|
|3c5469|
|3f6533|
|3c4346|
|3c56e7|
|3c48f0|
|3c56eb|
+------+
only showing top 20 rows
```

### f. Show top 10 origin_country with the highest number of unique airplanes in air for the last day
```
df.filter(allAfter(24) && col("on_ground") === "False").groupBy("origin_country").count.sort(col("count").desc).limit(10).show
+--------------+-------+
|origin_country|  count|
+--------------+-------+
| United States|1258911|
|United Kingdom| 330613|
|       Ireland| 191584|
|       Germany| 171816|
|         China| 157425|
|        Turkey| 101521|
|        Canada|  81760|
|         Spain|  78357|
|        France|  78136|
|       Austria|  72958|
+--------------+-------+
```

### g. Show top 10 longest (by time) completed flights for the last day
???

### h. Get the average geo_altitude value for each origin_country(round the result to 3 decimal places and rename column)
```
df.groupBy("origin_country").agg(round(avg("geo_altitude"), 3)).withColumnRenamed("round(avg(geo_altitude), 3)", "avg_geo_altitude").show
+--------------------+----------------+
|      origin_country|avg_geo_altitude|
+--------------------+----------------+
|            Paraguay|            null|
|               Yemen|        8499.142|
|Islamic Republic ...|        8403.313|
|             Senegal|        7173.151|
|              Sweden|         6461.14|
|   Republic of Korea|        8639.136|
|         Philippines|         6367.23|
|           Singapore|          9290.4|
|            Malaysia|        6314.988|
|Kingdom of the Ne...|        8301.189|
|                Fiji|        9343.412|
|              Turkey|        9233.376|
|                Iraq|        8013.318|
|             Germany|        7411.535|
|         Afghanistan|       11381.801|
|            Cambodia|        5874.098|
|              Jordan|        9890.289|
|              Rwanda|       12167.801|
|            Maldives|            null|
|              France|        8076.803|
+--------------------+----------------+
only showing top 20 rows
```