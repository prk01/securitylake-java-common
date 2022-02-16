package org.securitylake.common.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test
 */
public class ReferenceTest
{
    @Test
    public void testReference()
    {
        Reference<String> refString = new Reference<>(null);
        Assert.assertNull(refString.getT());
        refString.setT("str1");
        Assert.assertEquals(refString.getT(), "str1");
        Assert.assertEquals("Reference {t=str1}", refString.toString());
    }
}
