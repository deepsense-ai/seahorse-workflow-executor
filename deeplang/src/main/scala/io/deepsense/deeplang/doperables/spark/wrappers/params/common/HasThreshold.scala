/**
 * Copyright 2015, deepsense.io
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

package io.deepsense.deeplang.doperables.spark.wrappers.params.common

import io.deepsense.deeplang.params.Params
import io.deepsense.deeplang.params.validators.RangeValidator
import io.deepsense.deeplang.params.wrappers.spark.DoubleParamWrapper
import org.apache.spark.ml

import scala.language.reflectiveCalls

trait HasThreshold extends Params {

  val threshold = new DoubleParamWrapper[ml.param.Params { val threshold: ml.param.DoubleParam }](
    name = "threshold",
    description = "Threshold in binary classification prediction, in range [0, 1]",
    sparkParamGetter = _.threshold,
    validator = RangeValidator(0.0, 1.0))
  setDefault(threshold, 0.5)
}
