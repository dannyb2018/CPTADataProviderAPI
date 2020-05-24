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

import com.cloudpta.quantpipeline.api.instrument.symbology.CPTAInstrumentSymbology;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataField;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataMessage;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataProperty;
import com.cloudpta.quantpipeline.backend.data_provider.request_response.CPTADataRetriever;
import com.cloudpta.utilites.exceptions.CPTAException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.json.JsonArray;
import javax.json.JsonObject;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Danny
 */
public class CPTADataProviderProcessorTest
{
    /**
     * Test of setUpDataRetriever method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testSetUpDataRetriever()
    {
        System.out.println("setUpDataRetriever");
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        instance.setUpDataRetriever();
    }

    /**
     * Test of addProperties method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testAddProperties()
    {
        System.out.println("addProperties");
        List<PropertyDescriptor> thisInstanceDescriptors = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        instance.addProperties(thisInstanceDescriptors);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of init method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testInit()
    {
        System.out.println("init");
        ProcessorInitializationContext context = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        instance.init(context);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getRelationships method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetRelationships()
    {
        System.out.println("getRelationships");
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        Set<Relationship> expResult = null;
        Set<Relationship> result = instance.getRelationships();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getSupportedPropertyDescriptors method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetSupportedPropertyDescriptors()
    {
        System.out.println("getSupportedPropertyDescriptors");
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        List<PropertyDescriptor> expResult = null;
        List<PropertyDescriptor> result = instance.getSupportedPropertyDescriptors();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of onScheduled method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testOnScheduled()
    {
        System.out.println("onScheduled");
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        System.out.println("OnScheduled not implemented, not sure what it does");
    }

    /**
     * Test of onPropertyModified method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testOnPropertyModified()
    {
        System.out.println("onPropertyModified");
        PropertyDescriptor descriptor = null;
        String oldValue = "";
        String newValue = "";
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        instance.onPropertyModified(descriptor, oldValue, newValue);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of onTrigger method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testOnTrigger()
    {
        System.out.println("onTrigger");

        String emptyRequestString = "{\""+ CPTADataProviderAPIConstants.INSTRUMENTS_ARRAY_NAME + "\":[], \""+ CPTADataProviderAPIConstants.FIELDS_ARRAY_NAME + "\":[], \""+ CPTADataProviderAPIConstants.PROPERTIES_ARRAY_NAME + "\":[]}";
        InputStream content = new ByteArrayInputStream(emptyRequestString.getBytes());

        CPTADataProviderProcessor instance = new CPTADummyProcessor();

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(instance);

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(CPTADataProviderAPIConstants.RELATIONSHIP_NAME_SUCCESS);
        assertTrue(results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));

        // Test attributes and content
//        result.assertAttributeEquals(CPTADSSDataProviderProcessor.MATCH_ATTR, "nifi rocks");
        result.assertContentEquals("nifi rocks");       
    }

    /**
     * Test of getInstruments method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetInstruments() throws Exception
    {
        System.out.println("getInstruments");
        JsonObject request = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        List<CPTAInstrumentSymbology> expResult = null;
        List<CPTAInstrumentSymbology> result = instance.getInstruments(request);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getRequestFields method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetRequestFields() throws Exception
    {
        System.out.println("getRequestFields");
        JsonObject request = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        List<CPTADataField> expResult = null;
        List<CPTADataField> result = instance.getRequestFields(request);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getRequestProperties method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetRequestProperties() throws Exception
    {
        System.out.println("getRequestProperties");
        JsonObject request = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        List<CPTADataProperty> expResult = null;
        List<CPTADataProperty> result = instance.getRequestProperties(request);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getData method, of class CPTADataProviderProcessor.
     */
    @Test
    public void testGetData() throws Exception
    {
        System.out.println("getData");
        ProcessContext context = null;
        List<CPTAInstrumentSymbology> symbols = null;
        List<CPTADataField> fields = null;
        List<CPTADataProperty> properties = null;
        CPTADataProviderProcessor instance = new CPTADummyProcessor();
        String expResult = "";
        String result = instance.getData(context, symbols, fields, properties);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    public class CPTADummyProcessor extends CPTADataProviderProcessor<CPTADataRetriever>
    {
        @Override
        public void setUpDataRetriever()
        {
            HashMap<String, Class> typeToMessageClassMap = new HashMap<>();

            // Set up the mapper
            typeToMessageClassMap = new HashMap<>();
            // Populate with types


            dataRetriever = CPTADataRetriever.getInstance(typeToMessageClassMap);        
        }

        @Override
        public void addProperties(List<PropertyDescriptor> thisInstanceDescriptors)
        {
        }
    }
    
    public class CPTADummyDataMessage extends CPTADataMessage
    {
        @Override
        public JsonArray getResult(ComponentLog logger, ProcessContext context, List<CPTAInstrumentSymbology> symbols, List<String> fields, List<CPTADataProperty> properties) throws CPTAException
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getMessageType()
        {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }

    
}