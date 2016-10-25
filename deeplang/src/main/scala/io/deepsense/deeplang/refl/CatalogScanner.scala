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

package io.deepsense.deeplang.refl

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}

import org.reflections.Reflections

import io.deepsense.commons.utils.Logging
import io.deepsense.deeplang.catalogs.doperable.DOperableCatalog
import io.deepsense.deeplang.catalogs.doperations.DOperationsCatalog
import io.deepsense.deeplang.{DOperable, DOperation, DOperationCategories, TypeUtils}

object CatalogScanner extends Logging {

  /**
    * Scans jars on classpath for classes annotated with [[io.deepsense.deeplang.refl.Register Register]]
    * annotation and at the same time implementing [[io.deepsense.deeplang.DOperation DOperation]]
    * or [[io.deepsense.deeplang.DOperable DOperable]] interfaces. Found classes are then registered
    * in appropriate catalogs.
    *
    * @see [[io.deepsense.deeplang.refl.Register Register]]
    */
  def scanAndRegister(
      dOperableCatalog: DOperableCatalog,
      dOperationsCatalog: DOperationsCatalog
    ): Unit = {
    for (registrable <- scanForRegistrables()) {
      logger.debug(s"Trying to register class $registrable")
      registrable match {
        case DOperableMatcher(doperable) => registerDOperable(dOperableCatalog, doperable)
        case DOperationMatcher(doperation) => registerDOperation(dOperationsCatalog, doperation)
        case other => logger.warn(s"Only DOperable and DOperation can be `@Register`ed")
      }
    }
  }

  private def scanForRegistrables() : Set[Class[_]] = {
    val reflections = new Reflections()
    reflections.getTypesAnnotatedWith(classOf[Register]).toSet
  }

  private def registerDOperation(
    catalog: DOperationsCatalog,
    operation: Class[DOperation]
  ): Unit = TypeUtils.constructorForClass(operation) match {
    case Some(constructor) =>
      catalog.registerDOperation(
        DOperationCategories.UserDefined,
        () => TypeUtils.createInstance[DOperation](constructor)
      )
    case None => logger.error(
      s"Class $operation could not be registered." +
        "It needs to have parameterless constructor"
    )
  }

  private def registerDOperable(
    catalog: DOperableCatalog,
    operable: Class[DOperable]
  ): Unit = catalog.register(TypeUtils.classToType(operable))

  class AssignableFromExtractor[T](targetClass: Class[T]) {
    def unapply(clazz: Class[_]): Option[Class[T]] = {
      if (targetClass.isAssignableFrom(clazz)) {
        Some(clazz.asInstanceOf[Class[T]])
      } else {
        None
      }
    }
  }

  object DOperableMatcher extends AssignableFromExtractor(classOf[DOperable])
  object DOperationMatcher extends AssignableFromExtractor(classOf[DOperation])

}