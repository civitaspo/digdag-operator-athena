package pro.civitaspo.digdag.plugin.athena.util


import com.google.common.base.Optional
import io.digdag.spi.SecretProvider


class SecretProviderChain(a: SecretProvider,
                          b: SecretProvider)
    extends SecretProvider
{
    override def getSecretOptional(key: String): Optional[String] =
    {
        a.getSecretOptional(key).or(b.getSecretOptional(key))
    }
}

object SecretProviderChain
{
    def apply(secretProvider: SecretProvider,
              secretProviders: SecretProvider*): SecretProvider =
    {
        secretProviders.foldLeft(secretProvider) { (a,
                                                    b) => new SecretProviderChain(a, b)
        }
    }
}
