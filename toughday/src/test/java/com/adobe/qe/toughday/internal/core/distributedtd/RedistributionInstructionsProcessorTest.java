package com.adobe.qe.toughday.internal.core.distributedtd;

import com.adobe.qe.toughday.MockTest;
import com.adobe.qe.toughday.internal.core.ReflectionsContainer;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.RedistributionInstructions;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.RedistributionInstructionsProcessor;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class RedistributionInstructionsProcessorTest {
    private List<String> cmdLineArgs;
    private final RedistributionInstructionsProcessor instructionsProcessor = new RedistributionInstructionsProcessor();
    private static ReflectionsContainer reflections = ReflectionsContainer.getInstance();

    @BeforeClass
    public static void onlyOnce() {
        System.setProperty("logFileName", ".");
        ((LoggerContext) LogManager.getContext(false)).reconfigure();

        reflections.getTestClasses().put("MockTest", MockTest.class);
    }

    @Before
    public void before() {
        cmdLineArgs = new ArrayList<>(Collections.singletonList("--host=localhost"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPhaseThrowsExceptions() throws IOException {
        this.instructionsProcessor.processInstructions("", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInstructionsThrowsException() throws Exception{
        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        this.instructionsProcessor.processInstructions(null, configuration.getPhases().get(0));
    }

}
