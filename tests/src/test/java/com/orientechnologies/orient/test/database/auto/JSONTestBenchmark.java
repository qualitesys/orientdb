/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(0)
public class JSONTestBenchmark extends DocumentDBBaseTest {
  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("JSONTestBenchmark.testAlmostLink*")
            // .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            // .addProfiler(GCProfiler.class)
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    final Collection<RunResult> results = new Runner(opt).run();

    /*final TDoubleArrayList xData = new TDoubleArrayList(scaleResults.keySet().size());
    scales.forEach(xData::add);

    final Map<String, TDoubleArrayList> yData = new HashMap<>();
    final Map<String, TDoubleArrayList> errorData = new HashMap<>();

    final List<SeriesBundle> allData = new ArrayList();

    for (final RunResult runResult : results) {
      final String name = runResult.getParams().getBenchmark();
      String benchmarkName = name.substring(name.lastIndexOf(".",name.lastIndexOf(".")-1) + 1);

      TDoubleArrayList yValues = yData.get(benchmarkName);
      if (yValues == null) {
        yValues = new TDoubleArrayList();
        yData.put(benchmarkName, yValues);
      }
      yValues.add(runResult.getPrimaryResult().getScore());

      TDoubleArrayList errorValues = errorData.get(benchmarkName);
      if (errorValues == null) {
        errorValues = new TDoubleArrayList();
        errorData.put(benchmarkName, errorValues);
      }
      errorValues.add(runResult.getPrimaryResult().getScoreError());
    }

    final Chart chart = createChart("", getParam(scaleResults, "scaleName"), getParam(scaleResults, "scaleUnit"));

    for (final String seriesName : yData.keySet()) {
      final ScaleSeriesBundle currentSeries = new ScaleSeriesBundle(seriesName,xData,yData.get(seriesName), null, errorData.get(seriesName), true);
      allData.add(currentSeries);
    }*/
  }

  @Benchmark
  public void testAlmostLink() {
    final ODocument doc = new ODocument();
    doc.fromJSON("{'title': '#330: Dollar Coins Are Done'}");
  }

  @Benchmark
  public void testAlmostLinkStream() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        new ByteArrayInputStream(
            "{'title': '#330: Dollar Coins Are Done'}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testNullList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [\"string\", null]}");
  }

  @Benchmark
  public void testNullListStream() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [\"string\", null]}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testBooleanList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [true, false]}");
  }

  @Benchmark
  public void testBooleanListStream() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [true, false]}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testNumericIntegerList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [17,42]}");
  }

  @Benchmark
  public void testNumericIntegerListStream() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [17,42]}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testNumericLongList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [100000000000,100000000001]}");
  }

  @Benchmark
  public void testNumericLongListStream() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [100000000000,100000000001]}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testNumericFloatList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [17.3,42.7]}");
  }

  @Benchmark
  public void testNumericFloatListStream() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [17.3,42.7]}".getBytes(StandardCharsets.UTF_8)));
  }

  @Benchmark
  public void testNullity() {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\","
            + "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith Ave\","
            + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},"
            + "\"dob\":\"2011-11-17 03:17:04\"}");
  }

  @Benchmark
  public void testNullityStream() throws IOException {
    final ODocument doc = new ODocument();
    final String json =
        "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\","
            + "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith Ave\","
            + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},"
            + "\"dob\":\"2011-11-17 03:17:04\"}";
    doc.fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }
}