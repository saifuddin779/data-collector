from itertools import cycle
import skiff

class droplets_manager(object):
	def __init__(self):
		self.token = 'b57edf366525324117fdcf42a1fe433327763ecae070c9ac01519ff4e5b0dab3'
		self.skiffer = skiff.rig(self.token)
		self.regions = cycle(['nyc1', 'nyc2', 'nyc3'])

	def create_droplet(self, name, slug_size='1gb'):
		droplet = self.skiffer.Droplet.create(
										{
											'name': name,
	                               			'region': self.regions.next(),
	                               			'image': 'ubuntu-14-04-x64',
	                               			'size':  slug_size,
	                               			'ssh_keys': self.skiffer.Key.all()
	                               		}
                               		)
		droplet.wait_till_done()
		droplet = droplet.refresh()
		name = droplet.name
		for k in droplet.v4:
			ip_address = k.ip_address

		#--now send the config file to it and prepare it to work

		return {'name': droplet.name, 'ip': ip_address}

	def destroy_droplet(self, name):
		pass


