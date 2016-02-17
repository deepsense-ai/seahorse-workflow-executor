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

package io.deepsense.deeplang.doperations

import scala.reflect.runtime.{universe => ru}

import io.deepsense.commons.utils.Version
import io.deepsense.deeplang.DOperation._
import io.deepsense.deeplang.doperables.Transformer
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.{DOperation1To0, ExecutionContext}

case class DeployTransformer()
  extends DOperation1To0[Transformer] {

  override val id: Id = "eeceebdc-d567-11e5-ab30-625662870761"
  override val name: String = "Deploy Transformer"
  override val description: String = "Deploys a Transformer on external server"

  override val since: Version = Version(1, 0, 0)

  override val params = declareParams()

  override protected def _execute(context: ExecutionContext)(t: Transformer): Unit = ()

  @transient
  override lazy val tTagTI_0: ru.TypeTag[Transformer] = ru.typeTag[Transformer]
}
