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
package com.dremio.sabot.op.filter;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

public interface Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filterer.class);

  public void setup(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) throws SchemaChangeException;
  public int filterBatch(int recordCount);

  public static TemplateClassDefinition<Filterer> TEMPLATE_DEFINITION2 = new TemplateClassDefinition<Filterer>(Filterer.class, FilterTemplate2.class);

  /* Alternative template which offloads evaluation of the filter to C++ code */
  public static TemplateClassDefinition<Filterer> TEMPLATE_ACCELERATED = new TemplateClassDefinition<Filterer>(Filterer.class, FilterTemplateAccelerated.class);

}
