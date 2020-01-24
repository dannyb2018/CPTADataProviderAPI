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

import com.cloudpta.quantpipeline.api.instrument.CPTAInstrumentConstants;
import com.cloudpta.quantpipeline.api.instrument.symbology.CPTAInstrumentSymbology;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataField;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataRetriever;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataProperty;
import com.cloudpta.utilites.exceptions.CPTAException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

/**
 *
 * @author Danny
 */
public abstract class CPTADataProviderProcessor<T extends CPTADataRetriever> extends AbstractProcessor 
{
    protected abstract void setUpDataRetriever();
    public abstract void addProperties(List<PropertyDescriptor> thisInstanceDescriptors);
            
    @Override
    protected void init(final ProcessorInitializationContext context) 
    {
        // Build a list of properties
        final List<PropertyDescriptor> thisInstanceDescriptors = new ArrayList<>();
        // Call the overriden method to add properties
        addProperties(thisInstanceDescriptors);
        descriptors = Collections.unmodifiableList(thisInstanceDescriptors);

        final Set<Relationship> thisInstanceRelationships = new HashSet<>();
        thisInstanceRelationships.add(SUCCESS);
        thisInstanceRelationships.add(FAILURE);
        // The two relationships are success and fail
        relationships = Collections.unmodifiableSet(thisInstanceRelationships);
        
        // Set up the interface to get the data
        setUpDataRetriever();
    }

    @Override
    public Set<Relationship> getRelationships() 
    {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() 
    {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) 
    {
        
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue)
    {
        super.onPropertyModified(descriptor, oldValue, newValue); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException 
    {
        // Always needs an incoming flow file
        FlowFile flowFile = session.get();
        if ( flowFile == null ) 
        {
            return;
        }

        // Has to be like this so we can access from within a lambda
        final AtomicReference<String> results = new AtomicReference<>();
        final AtomicReference<Relationship> statusOfQuery = new AtomicReference<>();
        // Has to be written like this otherwise will complain flow file isnt shut properly
        session.read
        (flowFile, in ->
            {
                try
                {
                    // Read in the json
                    JsonReader reader = Json.createReader(in);
                    JsonObject request = reader.readObject();

                    // Format is instruments, which is a simple array of json objects
                    // with identifier and identifier type
                    final List<CPTAInstrumentSymbology> instrumentsArray = getInstruments(request);

                    // Fields are array of field objects, 
                    // which are message type and field
                    List<CPTADataField> fieldsArray = getRequestFields(request);

                    // Finally there is the properties array
                    List<CPTADataProperty> requestPropertiesArray = getRequestProperties(request);
                    // Get data from refinitiv
                    String queryResults = getData(context, instrumentsArray, fieldsArray, requestPropertiesArray);
                    results.set(queryResults);
                    statusOfQuery.set(SUCCESS);
                }
                catch(CPTAException internalException)
                {
                    // Then there is an error
                    String error = internalException.getErrorsAsString();
                    results.set(error);
                    statusOfQuery.set(FAILURE);
                }
                catch(Exception unexpectedException)
                {
                    // There is another sort of error
                    CPTAException convertedException = new CPTAException(unexpectedException);
                    String error = convertedException.getErrorsAsString();
                    results.set(error);
                    statusOfQuery.set(FAILURE);                    
                }
                finally
                {
                    try
                    {
                        in.close();
                    }
                    catch(Exception E2)
                    {

                    }
                }
            }
        );

        // Write the results back out to flow file
        // Has to be written like this otherwise will complain flow file isnt shut properly
        session.write
        (
            flowFile, out ->
            {
                try
                {
                    String resultsOfQuery = results.get();
                    out.write(resultsOfQuery.getBytes());
                }
                catch(IOException E)
                {

                }
                finally
                {
                    try
                    {
                        out.close();
                    }
                    catch(Exception E2)
                    {

                    }
                }
            }
        );

        // pass on the flow file along with whether the query succeeded or not
        session.transfer(flowFile, statusOfQuery.get());
    }
    
    
    protected List<CPTAInstrumentSymbology> getInstruments(JsonObject request) throws CPTAException
    {
        // Convert this list of rics to a list of symbols with type as rics
        List<CPTAInstrumentSymbology> instruments = new ArrayList<>();
        
        // Should check that the json is properly formed at each stage otherwise throw an exception
        try
        {
            // Get the rics from the request, it is an array of json objects of instrument symbologies
            JsonArray instrumentsAsArray = request.getJsonArray(CPTADataProviderAPIConstants.INSTRUMENTS_ARRAY_NAME); 
            // If there are no instruments
            if(null == instrumentsAsArray)
            {
                List<String> errors = new ArrayList<>();
                errors.add("There are no instruments in this request");
                CPTAException e = new CPTAException(errors);
                throw e;                
            }

            // Get the list so we iterate easily over it
            List<JsonObject> instrumentsAsJsonObjects = instrumentsAsArray.getValuesAs(JsonObject.class);        
            for(JsonObject currentInstrument: instrumentsAsJsonObjects)
            {
                // Create a symbology entry for this ric
                CPTAInstrumentSymbology currentInstrumentSymbology = new CPTAInstrumentSymbology();
                // get id

                String id = currentInstrument.getString(CPTAInstrumentConstants.ID_FIELD_NAME);     
                currentInstrumentSymbology.setID(id);
                // get id type
                String idType = currentInstrument.getString(CPTAInstrumentConstants.ID_SOURCE_FIELD_NAME);
                currentInstrumentSymbology.setIDSource(idType);
                // Add to list of instruments
                instruments.add(currentInstrumentSymbology);
            }        
        }
        catch(NullPointerException ne)
        {
            List<String> errors = new ArrayList<>();
            errors.add("one of the instruments has an invalid format, each instrument should be json object with 'Identifier' and 'IdentifierType' fields");
            CPTAException e = new CPTAException(errors);
            throw e;
        }
        
        // Return the list
        return instruments;
    }
    
    protected List<CPTADataField> getRequestFields(JsonObject request) throws CPTAException
    {
        // need to convert this from a json array of json objects to a list of fields
        List<CPTADataField> fields = new ArrayList<>();

        // Should check that the json is properly formed at each stage otherwise throw an exception
        try
        {
            // Get the fields from the request, it is an array of json objects representing the request fields
            JsonArray fieldsAsArray = request.getJsonArray(CPTADataProviderAPIConstants.FIELDS_ARRAY_NAME);
            // If there are no fields
            if(null == fieldsAsArray)
            {
                List<String> errors = new ArrayList<>();
                errors.add("There are no fields in this request");
                CPTAException e = new CPTAException(errors);
                throw e;                
            }

            // Get the json array as a list so we can iterate over it
            List<JsonObject> fieldsAsJsonObjects = fieldsAsArray.getValuesAs(JsonObject.class);
            for( JsonObject currentRequestFieldObject: fieldsAsJsonObjects)
            {
                // Turns the json object into a list of fields with name and message type
                CPTADataField currentField = new CPTADataField();
                currentField.messageType = currentRequestFieldObject.getString(CPTADataProviderAPIConstants.MESSAGE_TYPE_FIELD_NAME);
                currentField.name = currentRequestFieldObject.getString(CPTADataProviderAPIConstants.FIELD_NAME_FIELD_NAME);
                // Add it to list
                fields.add(currentField);
            }
        }
        catch(NullPointerException ne)
        {
            List<String> errors = new ArrayList<>();
            errors.add("one of the fields has an invalid format, each field should be json object with 'name' and 'type' fields");
            CPTAException e = new CPTAException(errors);
            throw e;
        }
        
        return fields;
    }
    
    protected List<CPTADataProperty> getRequestProperties(JsonObject request) throws CPTAException
    {
        // need to convert this from a json array of json objects to a list of properties
        List<CPTADataProperty> properties = new ArrayList<>();

        try
        {
            // Get the properties from the request, it is an array of json objects representing the request fields
            JsonArray propertiesAsArray = request.getJsonArray(CPTADataProviderAPIConstants.PROPERTIES_ARRAY_NAME);
            // If there are no fields
            if(null == propertiesAsArray)
            {
                List<String> errors = new ArrayList<>();
                errors.add("There is no properties array in this request, to specify no properties use the empty array '[]'");
                CPTAException e = new CPTAException(errors);
                throw e;                
            }

            // Get the json array as a list so we can iterate over it
            List<JsonObject> propertiesAsJsonObjects = propertiesAsArray.getValuesAs(JsonObject.class);
            for( JsonObject currentRequestPropertyObject: propertiesAsJsonObjects)
            {
                // Turns the json object into a list of properties with name and value
                CPTADataProperty currentProperty = new CPTADataProperty();
                currentProperty.name = currentRequestPropertyObject.getString(CPTADataProviderAPIConstants.PROPERTY_NAME_FIELD_NAME);
                currentProperty.value = currentRequestPropertyObject.getString(CPTADataProviderAPIConstants.PROPERTY_VALUE_FIELD_NAME);
                // Add it to list
                properties.add(currentProperty);
            }
        }
        catch(NullPointerException ne)
        {
            List<String> errors = new ArrayList<>();
            errors.add("one of the properties has an invalid format, each property should be json object with 'name' and 'value' fields");
            CPTAException e = new CPTAException(errors);
            throw e;
        }
        
        return properties;
    }
    
    protected String getData(ProcessContext context, List<CPTAInstrumentSymbology> symbols, List<CPTADataField> fields, List<CPTADataProperty> properties) throws CPTAException
    {
        ComponentLog log = this.getLogger();
        String result = dataRetriever.getData(log, context, symbols, fields, properties);
        return result;
    }        
    
    
    protected T dataRetriever = null;
    protected String dataSource = null;
    
    protected List<PropertyDescriptor> descriptors;
        
    
    protected Set<Relationship> relationships;
    protected static final Relationship SUCCESS = new Relationship
    .Builder()
    .name(CPTADataProviderAPIConstants.RELATIONSHIP_NAME_SUCCESS)
    .description("Got data from data source")
    .build();
    
    protected static final Relationship FAILURE = new Relationship
    .Builder()
    .name(CPTADataProviderAPIConstants.RELATIONSHIP_NAME_FAILURE)
    .description("failed to get data from data source")
    .build();           
}
