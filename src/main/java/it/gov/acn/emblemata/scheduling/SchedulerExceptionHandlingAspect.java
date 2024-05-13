package it.gov.acn.emblemata.scheduling;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Aspect to handle exceptions thrown by scheduled tasks.
 * It's important to catch exceptions in scheduled tasks to avoid them being silently swallowed by the scheduler.
 * which would prevent the task from running again.
 */
@Aspect
@Component
public class SchedulerExceptionHandlingAspect {
    private final Logger logger = LoggerFactory.getLogger(SchedulerExceptionHandlingAspect.class);

    @Around("@annotation(org.springframework.scheduling.annotation.Scheduled)")
    public void around(ProceedingJoinPoint pjp) throws Throwable {
        try {
            pjp.proceed();
        } catch (Exception e) {
            handleException(e,pjp.getSignature().getName());
        }
    }

    private void handleException(Exception e, String pjpName) {
        logger.error("Error in scheduled task {}", pjpName, e);
    }
}