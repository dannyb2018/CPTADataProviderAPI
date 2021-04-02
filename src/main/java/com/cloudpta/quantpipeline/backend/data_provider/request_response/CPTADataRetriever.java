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
package com.cloudpta.quantpipeline.backend.data_provider.request_response;

import com.cloudpta.quantpipeline.api.instrument.symbology.CPTAInstrumentSymbology;
import com.cloudpta.utilites.exceptions.CPTAException;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;

/**
 *
 * @author Danny
 */
public class CPTADataRetriever
{
    public static CPTADataRetriever getInstance(HashMap<String, Class> mappingOfTypeToMessageClass)
    {
        if(null == dataGetterInstance)
        {
            dataGetterInstance = new CPTADataRetriever(mappingOfTypeToMessageClass);
        }
        
        return dataGetterInstance;
    }
    
    public String getData(ComponentLog logger, ProcessContext context, List<CPTAInstrumentSymbology> symbols, List<CPTADataField> fields, List<CPTADataProperty> properties) throws CPTAException
    {
        // Get the list of message types along with the fields for each message type
        HashMap<String,List<String>> mappedFields = getMappedFields(fields);
        
        JsonArrayBuilder responses = Json.createArrayBuilder();
        Set<String> messagesTypesToQuery = mappedFields.keySet();
        // Go through the list of message types
        for(String currentMessageType : messagesTypesToQuery )
        {
            // Get fields for the message type
            List<String> fieldsForThisMessageType = mappedFields.get(currentMessageType);
            // Try to create an instance of that message
            try
            {
                CPTADataMessage message = getMessageByType(currentMessageType);
                // For each message pass in the relevant request
                // Get the data which will be added to responses
                message.getResult
                                (
                                logger,
                                context, 
                                responses,
                                symbols, 
                                fieldsForThisMessageType,
                                properties
                                );
            } 
            catch(CPTAException internalException)
            {
                // rethrow it
                throw internalException;
            }
            catch(Exception E)
            {
                // Think what to do here
                E.printStackTrace();
            }
        }
        
        // Turn reponses into a proper response
        String globalResponse = createGlobalResponseFromList(responses);
        return globalResponse;
    }
    
    protected HashMap<String,List<String>> getMappedFields(List<CPTADataField> fields)
    {
        HashMap<String,List<String>> mappedFields = new HashMap<>();
        // for each type get the fields
        // Loop through the fields
        for( CPTADataField field : fields)
        {
            // Get the message type
            String messageType = field.messageType;
            // If it is not already in the map
            List<String> fieldsForThisType = mappedFields.get(messageType);
            if(null == fieldsForThisType)
            {
                // Add it
                fieldsForThisType = new ArrayList<>();
                mappedFields.put(messageType, fieldsForThisType);
            }
            
            // Add the field
            fieldsForThisType.add(field.name);
        }

        return mappedFields;
    }
    
    protected CPTADataMessage getMessageByType
                                             (
                                             String messageType
                                             ) 
                                             throws 
                                             InstantiationException, 
                                             IllegalAccessException
    {
        // Get class
        Class messageClassForThisType = typeToMessageClassMap.get(messageType);
        // Create an instance of it
        CPTADataMessage messageForThisType = (CPTADataMessage)(messageClassForThisType.newInstance());
        
        return messageForThisType;
    }
                                                         
    protected String createGlobalResponseFromList(JsonArrayBuilder responsesFromEachMessage)
    {        
        // Turn it into a string
        String globalResponseAsString = responsesFromEachMessage.build().toString();
        // Return that string        
        return globalResponseAsString;
    }
    
    protected CPTADataRetriever(HashMap<String, Class> mappingOfTypeToMessageClass)
    {
        // private so we dont accidentally create this externally
        
        // Set up the mapper
        typeToMessageClassMap = mappingOfTypeToMessageClass;
    }
    
    static CPTADataRetriever dataGetterInstance = null;
    HashMap<String, Class> typeToMessageClassMap = null;
    
}
