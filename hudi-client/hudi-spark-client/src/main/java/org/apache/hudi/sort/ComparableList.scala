/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sort

import org.apache.hudi.common.util.ValidationUtils


class ComparableList(private val list: List[Any]) extends Ordered[ComparableList] {

  override def compare(that: ComparableList[T]): Int = {
    val thisIterator = list.iterator
    val thatIterator = that.list.iterator

    while (thisIterator.hasNext && thatIterator.hasNext) {
      val cmp = elementOrdering.compare(thisIterator.next(), thatIterator.next())
      if (cmp != 0) return cmp
    }

    Ordering.Int.compare(this.list.length, that.list.length)
  }

  override def toString: String = {
    list.mkString("ComparableList(", ", ", ")")
  }

  override def compare(that: ComparableList): Int = {
    val thisIterator = list.iterator
    val thatIterator = that.list.iterator

    while (thisIterator.hasNext && thatIterator.hasNext) {
      val o1 = thisIterator.next()
      val o2 = thatIterator.next()
      // first check the same class
      ValidationUtils.checkArgument(o1.getClass == o2.getClass, s"Cannot compare different types of objects: $o1, $o2")
      val cmp =
      if (cmp != 0) return cmp
    }

    Ordering.Int.compare(this.list.length, that.list.length)
  }
}

object ComparableList {
  def apply[T: Ordering](list: List[T]): ComparableList[T] = new ComparableList(list)
}