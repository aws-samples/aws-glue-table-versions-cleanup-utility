package software.aws.glue.tableversions.utils;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;

public class Test {

	public static void main(String[] args) {
		AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().build();
		GetCallerIdentityRequest request = new GetCallerIdentityRequest();
		GetCallerIdentityResult response = client.getCallerIdentity(request);
		System.out.println("Account Id: " + response.getAccount());
		

	}

}
