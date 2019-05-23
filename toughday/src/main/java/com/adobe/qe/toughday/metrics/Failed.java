/*
Copyright 2015 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
package com.adobe.qe.toughday.metrics;

import com.adobe.qe.toughday.api.annotations.Description;
import com.adobe.qe.toughday.api.core.RunMap;
import io.prometheus.client.Counter;

@Description(desc = "Number of fails.")
public class Failed extends Metric {
    private PrometheusMetricFactory<Counter> prometheusMetricFactory = null;

    @Override
    public Object getValue(RunMap.TestStatistics testStatistics) {
        return testStatistics.getFailRuns();
    }

    @Override
    public PrometheusMetricFactory<Counter> getPrometheusMetricFactory() {
        if (prometheusMetricFactory == null) {
            prometheusMetricFactory = new PrometheusMetricFactory<Counter>() {
                @Override
                public Counter getPrometheusRepresentation(String phaseName, String testName) {
                    Counter counter =  Counter.build()
                            .name(phaseName + "_" + testName + "_" + getName())
                            .help("Number of failed tests")
                            .labelNames(phaseName, testName)
                            .create();
                    return counter;
                }

                @Override
                public void updatePrometheusObject(Object value, Counter promObject, String phaseName, String testName) {
                    if (promObject == null) {
                        System.out.println("Null counter given.");
                        return;
                    }

                    double newValue = Double.valueOf(value.toString());
                    promObject.labels(phaseName, testName)
                            .inc(newValue - promObject.labels(phaseName, testName).get());
                }
            };
        }

        return prometheusMetricFactory;
    }

    @Override
    public String getFormat() {
        return "%d";
    }

    @Override
    public String getUnitOfMeasure() {
        return "";
    }

}
