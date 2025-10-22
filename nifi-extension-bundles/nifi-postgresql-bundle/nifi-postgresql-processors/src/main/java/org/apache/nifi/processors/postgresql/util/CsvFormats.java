/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.postgresql.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

/** Utility helpers for constructing CSV formats consistently. */
public final class CsvFormats {
    private CsvFormats() {}

    public static CSVFormat buildCsvPrintFormat(final ProcessorProperties properties) {
        final String delimiter = properties.getCsvDelimiter();
        final String quote = properties.getCsvQuote();
        final String escape = properties.getCsvEscape();
        final String nullToken = properties.getCsvNullToken();
        return CSVFormat.POSTGRESQL_CSV.builder()
                .setDelimiter(delimiter.charAt(0))
                .setQuote(quote.charAt(0))
                .setEscape(escape.charAt(0))
                .setQuoteMode(QuoteMode.ALL_NON_NULL)
                .setNullString(nullToken)
                .build();
    }

    public static CSVFormat buildCsvParseFormat(final ProcessorProperties properties) {
        final String delimiter = properties.getCsvDelimiter();
        final String quote = properties.getCsvQuote();
        final String escape = properties.getCsvEscape();
        final String nullToken = properties.getCsvNullToken();
        return CSVFormat.POSTGRESQL_CSV.builder()
                .setDelimiter(delimiter.charAt(0))
                .setQuote(quote.charAt(0))
                .setEscape(escape.charAt(0))
                .setNullString(nullToken)
                .setSkipHeaderRecord(true)
                .build();
    }
}


