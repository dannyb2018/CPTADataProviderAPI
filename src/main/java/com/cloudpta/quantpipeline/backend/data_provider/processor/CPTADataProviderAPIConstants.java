/*

Copyright 2017-2019 Advanced Products Limited, 
dannyb@cloudpta.com
github.com/dannyb2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package com.cloudpta.quantpipeline.backend.data_provider.processor;

/**
 *
 * @author Danny
 */
public interface CPTADataProviderAPIConstants
{
    public static final String RELATIONSHIP_NAME_SUCCESS = "success";
    public static final String RELATIONSHIP_NAME_FAILURE = "failure";
    

    public static final String CPTA_START_DATE_PROPERTY = "start";
    public static final String CPTA_END_DATE_PROPERTY = "end";
    public static final String CPTA_FREQUENCY_PROPERTY = "frequency";
    public static final String CPTA_ADJUST_PRICES_PROPERTY = "adjust_prices";
    
    public static final String INSTRUMENTS_ARRAY_NAME = "instruments";
    public static final String FIELDS_ARRAY_NAME = "fields";
    public static final String PROPERTIES_ARRAY_NAME = "properties";
    
    public static final String MESSAGE_TYPE_FIELD_NAME = "type";
    public static final String FIELD_NAME_FIELD_NAME = "name";
    public static final String PROPERTY_NAME_FIELD_NAME = "name";
    public static final String PROPERTY_VALUE_FIELD_NAME = "value";    
}
