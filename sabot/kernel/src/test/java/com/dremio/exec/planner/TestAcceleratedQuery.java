/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.planner;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestAcceleratedQuery extends BaseTestQuery {

  // Query which can be evaluated in a single recordbatch
  @Test
  public void testTaxiQuery() throws Exception {
    String query = "SELECT SUM(Trip_Seconds) FROM cp.\"Taxi_Trips_300.parquet\" WHERE Company LIKE 'Blue Ribbon Taxi Association Inc.'";
    test(query);
  }

  // Query which is evaluated in multiple recordbatches
  @Test
  public void testBigTaxiQuery() throws Exception {
    String query = "SELECT SUM(Trip_Seconds) FROM cp.\"Taxi_Trips_1M.parquet\" WHERE Company LIKE 'Blue Ribbon Taxi Association Inc.'";
    test(query);
  }
}
