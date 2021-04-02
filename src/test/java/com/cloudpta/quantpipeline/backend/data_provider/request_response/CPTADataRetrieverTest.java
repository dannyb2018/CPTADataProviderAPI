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
import java.util.UUID;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Danny
 */
public class CPTADataRetrieverTest
{
    /**
     * Test of getInstance method, of class CPTARefinitivGetData.
     */
    @Test
    public void testGetInstance()
    {
        System.out.println("getInstance");
        // Set up the mapping of types to message classes
        HashMap<String, Class> mapping = new HashMap<>();
        mapping.put(CPTARandomTestDataMessage2.messageType, CPTARandomTestDataMessage2.class);
        mapping.put(CPTARandomTestDataMessage1.messageType, CPTARandomTestDataMessage1.class);
        
        CPTADataRetriever instance = CPTADataRetriever.getInstance(mapping);
        // Should have 2 message types
        assertEquals(instance.typeToMessageClassMap.size(),2);
        // Have random message 1
        Class messageClass = instance.typeToMessageClassMap.get(CPTARandomTestDataMessage1.messageType);
        assertEquals(messageClass.getTypeName(), CPTARandomTestDataMessage1.class.getTypeName());
        // Have random message 2
        messageClass = instance.typeToMessageClassMap.get(CPTARandomTestDataMessage2.messageType);
        assertEquals(messageClass.getTypeName(), CPTARandomTestDataMessage2.class.getTypeName());
    }


    /**
     * Test of getMappedFields method, of class CPTARefinitivGetData.
     */
    @Test
    public void testGetMappedFields()
    {
        System.out.println("getMappedFields");
        
        // Start with empty list
        List<CPTADataField> fields = new ArrayList<>();
        // Set up the mapping of types to message classes
        HashMap<String, Class> mapping = new HashMap<>();
        mapping.put(CPTARandomTestDataMessage2.messageType, CPTARandomTestDataMessage2.class);
        mapping.put(CPTARandomTestDataMessage1.messageType, CPTARandomTestDataMessage1.class);
        
        CPTADataRetriever instance = CPTADataRetriever.getInstance(mapping);
        HashMap<String, List<String>> result = instance.getMappedFields(fields);
        // Check it is empty
        assertTrue(result.isEmpty());
        
        // Make one of random message 1 message type
        fields = new ArrayList<>();
        CPTADataField field11 = new CPTADataField();
        field11.messageType = CPTARandomTestDataMessage1.messageType;
        field11.name = UUID.randomUUID().toString();
        fields.add(field11);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be just one message type
        Set<String> messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 1);
        // It should be random message type 1
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage1.messageType));
        // should have only one field
        List<String> resultFields = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields.isEmpty());
        assertEquals(resultFields.size(), 1);
        // its name should be what was set
        String resultField1Name = resultFields.get(0);
        assertEquals(resultField1Name, field11.name);
        
        // Make one of the DSWS message type
        fields = new ArrayList<>();
        CPTADataField field21 = new CPTADataField();
        field21.messageType = CPTARandomTestDataMessage2.messageType;
        field21.name = UUID.randomUUID().toString();
        fields.add(field21);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be just one message type
        messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 1);
        // It should be message type 2
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage2.messageType));
        // should have only one field
        List<String> resultFields2 = result.get(CPTARandomTestDataMessage2.messageType);
        assertTrue(false == resultFields2.isEmpty());
        assertEquals(resultFields2.size(), 1);
        // its name should be what was set
        String resultField1Name2 = resultFields2.get(0);
        assertEquals(resultField1Name2, field21.name);
        
        //  Make two of random message 1 message type
        fields = new ArrayList<>();
        field11 = new CPTADataField();
        field11.messageType = CPTARandomTestDataMessage1.messageType;
        field11.name = UUID.randomUUID().toString();
        fields.add(field11);
        CPTADataField field12 = new CPTADataField();
        field12.messageType = CPTARandomTestDataMessage1.messageType;
        field12.name = UUID.randomUUID().toString();
        fields.add(field12);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be just one message type
        messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 1);
        // It should be EOD
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage1.messageType));
        // should have two fields
        List<String> resultFields1 = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields1.isEmpty());
        assertEquals(resultFields1.size(),2);
        // its name should be what was set
        resultField1Name = resultFields1.get(0);
        assertEquals(resultField1Name, field11.name);
        String resultField2Name = resultFields1.get(1);
        assertEquals(resultField2Name, field12.name);

        // Make two of message type 2
        fields = new ArrayList<>();
        field21 = new CPTADataField();
        field21.messageType = CPTARandomTestDataMessage1.messageType;
        field21.name = UUID.randomUUID().toString();
        fields.add(field21);
        CPTADataField field22 = new CPTADataField();
        field22.messageType = CPTARandomTestDataMessage1.messageType;
        field22.name = UUID.randomUUID().toString();
        fields.add(field22);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be just one message type
        messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 1);
        // It should be random type2 
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage1.messageType));
        // should have two fields
        resultFields2 = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields2.isEmpty());
        assertEquals(resultFields2.size(), 2);
        // its name should be what was set
        resultField1Name = resultFields2.get(0);
        assertEquals(resultField1Name, field21.name);
        resultField2Name = resultFields2.get(1);
        assertEquals(resultField2Name, field22.name);
        
        // Make one of the random message 1 message type and one of random message 2 message type
        fields = new ArrayList<>();
        field11 = new CPTADataField();
        field11.messageType = CPTARandomTestDataMessage1.messageType;
        field11.name = UUID.randomUUID().toString();
        fields.add(field11);
        field21 = new CPTADataField();
        field21.messageType = CPTARandomTestDataMessage2.messageType;
        field21.name = UUID.randomUUID().toString();
        fields.add(field21);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be two message types
        messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 2);
        // Should include both random type 1 and 2
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage1.messageType));
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage2.messageType));
        resultFields1 = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields1.isEmpty());
        resultFields2 = result.get(CPTARandomTestDataMessage2.messageType);
        assertTrue(false == resultFields2.isEmpty());
        // Should be one random message 2 field
        resultFields2 = result.get(CPTARandomTestDataMessage2.messageType);
        assertTrue(false == resultFields2.isEmpty());
        assertEquals(resultFields2.size(), 1);
        resultField1Name = resultFields2.get(0);
        assertEquals(resultField1Name, field21.name);
        // Should be one random message 1 field
        resultFields1 = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields1.isEmpty());
        assertEquals(resultFields1.size(),1);
        // its name should be what was set
        String resultField11Name = resultFields1.get(0);
        assertEquals(resultField11Name, field11.name);
        
        // Make one of the random message1 message type and two of random message 2 message type
        fields = new ArrayList<>();
        field11 = new CPTADataField();
        field11.messageType = CPTARandomTestDataMessage1.messageType;
        field11.name = UUID.randomUUID().toString();
        fields.add(field11);
        field21 = new CPTADataField();
        field21.messageType = CPTARandomTestDataMessage2.messageType;
        field21.name = UUID.randomUUID().toString();
        fields.add(field21);
        field22 = new CPTADataField();
        field22.messageType = CPTARandomTestDataMessage2.messageType;
        field22.name = UUID.randomUUID().toString();
        fields.add(field22);
        result = instance.getMappedFields(fields);
        // Shouldnt be empty
        assertTrue( false == result.isEmpty());
        // Should be two message types
        messageTypes = result.keySet();
        assertTrue(false == messageTypes.isEmpty());
        assertEquals(messageTypes.size(), 2);
        // Should include both random type 1 and type 2
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage1.messageType));
        assertTrue(messageTypes.contains(CPTARandomTestDataMessage2.messageType));
        resultFields2 = result.get(CPTARandomTestDataMessage2.messageType);
        assertTrue(false == resultFields2.isEmpty());
        // Should be two fields of message type random 2
        resultFields2 = result.get(CPTARandomTestDataMessage2.messageType);
        assertTrue(false == resultFields2.isEmpty());
        assertEquals(resultFields2.size(), 2);
        String resultField21Name = resultFields2.get(0);
        assertEquals(resultField21Name, field21.name);
        String resultField22Name = resultFields2.get(1);
        assertEquals(resultField22Name, field22.name);
        // Should be one message type 1
        resultFields1 = result.get(CPTARandomTestDataMessage1.messageType);
        assertTrue(false == resultFields1.isEmpty());
        assertEquals(resultFields1.size(),1);
        // its name should be what was set
        resultField11Name = resultFields1.get(0);
        assertEquals(resultField11Name, field11.name);        
    }

    /**
     * Test of getMessageByType method, of class CPTARefinitivGetData.
     */
    @Test
    public void testGetMessageByType() throws Exception
    {
        System.out.println("getMessageByType");
        // Set up the mapping of types to message classes
        HashMap<String, Class> mapping = new HashMap<>();
        mapping.put(CPTARandomTestDataMessage2.messageType, CPTARandomTestDataMessage2.class);
        mapping.put(CPTARandomTestDataMessage1.messageType, CPTARandomTestDataMessage1.class);
        
        // Get the instance
        CPTADataRetriever instance = CPTADataRetriever.getInstance(mapping);
        // random message 1 
        CPTADataMessage messageClass = instance.getMessageByType(CPTARandomTestDataMessage1.messageType);
        assertTrue(messageClass instanceof CPTARandomTestDataMessage1);
        // random message 2
        messageClass = instance.getMessageByType(CPTARandomTestDataMessage2.messageType);
        assertTrue(messageClass instanceof CPTARandomTestDataMessage2);
    }

    /**
     * Test of createGlobalResponseFromList method, of class CPTARefinitivGetData.
     */
    @Test
    public void testCreateGlobalResponseFromList()
    {
        System.out.println("createGlobalResponseFromList");
        JsonArrayBuilder responsesFromEachMessage = Json.createArrayBuilder();
        // Set up the mapping of types to message classes
        HashMap<String, Class> mapping = new HashMap<>();
        mapping.put(CPTARandomTestDataMessage2.messageType, CPTARandomTestDataMessage2.class);
        mapping.put(CPTARandomTestDataMessage1.messageType, CPTARandomTestDataMessage1.class);
        
        // Get the instance
        CPTADataRetriever instance = CPTADataRetriever.getInstance(mapping);
        String expResult = "";
        String result = instance.createGlobalResponseFromList(responsesFromEachMessage);
        System.out.println(result);
    }
}

class CPTARandomTestDataMessage1 extends CPTADataMessage
{

    @Override
    public void getResult(ComponentLog logger, ProcessContext context, JsonArrayBuilder responses, List<CPTAInstrumentSymbology> symbols, List<String> fields, List<CPTADataProperty> properties) throws CPTAException
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMessageType()
    {
        return messageType;
    }
    
    static String messageType = UUID.randomUUID().toString();
    
}

class CPTARandomTestDataMessage2 extends CPTADataMessage
{

    @Override
    public void getResult(ComponentLog logger, ProcessContext context, JsonArrayBuilder responses, List<CPTAInstrumentSymbology> symbols, List<String> fields, List<CPTADataProperty> properties) throws CPTAException
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMessageType()
    {
        return messageType;
    }

    static String messageType = UUID.randomUUID().toString();    
}