/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.kademi.deploy;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author brad
 */
public class AppDeployerTest {
    
    public AppDeployerTest() {
    }

    @Test
    public void testIncrement_simple() {
        String s = AppDeployer.getIncrementedVersionNumber("1.0.0"); 
        assertEquals("1.0.1", s);
    }

        @Test
    public void testIncrement_higher() {
        String s = AppDeployer.getIncrementedVersionNumber("1.0.9"); 
        assertEquals("1.0.10", s);
    }

}
