package it.gov.acn.emblemata.unit;

import it.gov.acn.emblemata.scheduling.SchedulerExceptionHandlingAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

public class SchedulerExceptionHandlingAspectUnitTest {

    @Test
    public void testAround() throws Throwable {
        // Arrange
        ProceedingJoinPoint pjp = Mockito.mock(ProceedingJoinPoint.class);
        Signature signature = Mockito.mock(Signature.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(pjp.getSignature()).thenReturn(signature);
        Mockito.when(signature.getName()).thenReturn("mockScheduledMethod");
        Mockito.when(pjp.proceed()).thenThrow(new RuntimeException("Mock exception"));

        SchedulerExceptionHandlingAspect aspect = new SchedulerExceptionHandlingAspect();
        ReflectionTestUtils.setField(aspect, "logger", logger);

        // Act
        aspect.around(pjp);

        // Assert
        Mockito.verify(logger).error(Mockito.eq("Error in scheduled task {}"), Mockito.eq("mockScheduledMethod"), Mockito.any(RuntimeException.class));
    }
}