/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.cobrix.cobol.parser.examples.generators

import za.co.absa.cobrix.cobol.parser.examples.generators.model.Company

object CommonLists {

  val currencies: Seq[String] =
    Seq("ZAR", "ZAR", "ZAR", "ZAR", "ZAR", "ZAR", "ZAR", "ZAR", "USD", "EUR", "GBP", "CAD", "CHF", "CZK", "CYN")

  val companies: Seq[Company] = Seq(
    Company("ABCD Ltd.", "0039887123", "74 Lawn ave., New York"),
    Company("ECSRONO", "0039567812", "123/B Prome str., Denver"),
    Company("ZjkLPj", "0034412331", "5574, Tokyo"),
    Company("Eqartion Inc.", "0039003991", "871A Forest ave., Toronto"),
    Company("Test Bank", "0092317899", "1 Garden str., London"),
    Company("Pear GMBH.", "0002377771", "107 Labe str., Berlin"),
    Company("Beiereqweq.", "0123330087", "901 Ztt, Munich"),
    Company("Joan Q & Z", "0039887123", "10 Sandton, Johannesburg"),
    Company("Robotrd Inc.", "0039801988", "2 Park ave., Johannesburg"),
    Company("Beierbauh.", "0038903321", "2 G. str., Johannesburg"),
    Company("Delta Pivovar", "0021213441", "74 Staromestka., Prague"),
    Company("Xingzhoug", "8822278911", "74 Qing ave., Beijing")
  )

  val firstNames: Seq[String] = Seq(
    "Jene",
    "Maya",
    "Starr",
    "Lynell",
    "Eliana",
    "Tyesha",
    "Beatrice",
    "Otelia",
    "Timika",
    "Wilbert",
    "Mindy",
    "Sunday",
    "Tyson",
    "Cliff",
    "Mabelle",
    "Verdie",
    "Sulema",
    "Alona",
    "Suk",
    "Deandra",
    "Doretha",
    "Cassey",
    "Janiece",
    "Deshawn",
    "Willis",
    "Carrie",
    "Gabriele",
    "Inge",
    "Edyth",
    "Estelle"
  )

  val lastNames: Seq[String] = Seq(
    "Corle",
    "Mackinnon",
    "Mork",
    "Shapiro",
    "Boettcher",
    "Flatt",
    "Acuna",
    "Thorpe",
    "Riojas",
    "Lepe",
    "Maxim",
    "Gagliano",
    "Benally",
    "Ortego",
    "Winburn",
    "Sauve",
    "Concannon",
    "Newcombe",
    "Boehme",
    "Hisle",
    "Godfrey",
    "Wallingford",
    "Debow",
    "Bourke",
    "Deveau",
    "Batman",
    "Norgard",
    "Tumlin",
    "Celestin",
    "Brandis"
  )

}
