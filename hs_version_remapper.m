disp('Mapping COMTRADE tensor to new HS version...');

%% Initialise

% Set current directory
current_dir = [fileparts(which(mfilename)) filesep]; 
addpath(genpath(current_dir));

% Environment and settings
options = json_parser([current_dir 'config.json']);
meta = initialise_environment(options);

conc_dir = meta.conc_dir;
save_dir = meta.save_dir;

% Settings
flows = {'Import', 'Export'};
trade_units = {'$_CIF', '$_FOB', 'kg'};
timeseries = options.timeseries;
comtrade_dir = options.env.comtrade_dir;
base_classification = 'HS17';


disp('Complete.');