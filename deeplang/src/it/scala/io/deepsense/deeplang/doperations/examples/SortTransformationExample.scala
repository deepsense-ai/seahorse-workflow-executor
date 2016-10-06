/**
  * Copyright 2016, deepsense.io
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

package io.deepsense.deeplang.doperations.examples

import io.deepsense.deeplang.doperables.SortColumnParam
import io.deepsense.deeplang.doperations.SortTransformation

class SortTransformationExample extends AbstractOperationExample[SortTransformation] {
  override val fileNames = Seq("example_city_beds_price")

  override def dOperation: SortTransformation = {
    val op = new SortTransformation
    op.transformer.setColumns(Seq(
      SortColumnParam("city", descending = false),
      SortColumnParam("price", descending = true)
    ))
    op.set(op.transformer.extractParamMap())
  }
}
